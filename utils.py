import json
import logging
import os
import subprocess
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import youtube_dl
from pydub import AudioSegment

DATA_DIR_PATH = Path("/data")
DATA_DIR_TEMP_PATH = DATA_DIR_PATH / "temp"
DUPLICATE_LIST_FILE = DATA_DIR_PATH / "duplicates.csv"

Y_DL_FORMAT_STRING = "bestaudio/best"
MAX_Y_DL_RETRIALS = 3

LOGGER = logging.getLogger("MaloneyDownloader")


@dataclass
class Episode:
    title: str

    @property
    def final_path(self) -> Path:
        return DATA_DIR_PATH / f"{self.title}.mp3"

    @property
    def temp_path(self) -> Path:
        return DATA_DIR_TEMP_PATH / f"{self.title}.mp3"

    def move_from_temp_to_final(self):
        os.rename(src=self.temp_path, dst=self.final_path)


@dataclass
class SpotifyEpisode(Episode):
    songs: Dict[str, str]


@dataclass
class Drs3Episode(Episode):
    download_url: str


def extract_episodes_from_raw_songs(raw_songs: Dict[str, str]) -> List[SpotifyEpisode]:
    episodes: List[SpotifyEpisode] = []
    current_episode_title = ""
    current_episode: Optional[SpotifyEpisode] = None

    for raw_song_title, raw_song_artists in raw_songs.items():
        if ':' not in raw_song_title:
            # some episodes are not split into multiple tracks/scenes
            episode_title = raw_song_title
        else:
            episode_title, _scene_index = raw_song_title.split(':')
        if episode_title != current_episode_title:
            if current_episode:
                episodes.append(current_episode)
            current_episode = SpotifyEpisode(episode_title, songs={})
            current_episode_title = current_episode.title

        current_episode.songs[raw_song_title] = raw_song_artists

    if current_episode and current_episode not in episodes:
        episodes.append(current_episode)

    return episodes


def download_episode_from_yt(episode: SpotifyEpisode):
    """
    Downloads all (parts / "scenes") of the episodes, merges them (if necessary) and stores them in the data directory.
    Retries the download in case a connection error occurred.
    """
    episode_download_successful = True
    for song, artist in episode.songs.items():
        # Building the ydl_opts dict was taken from the spotify-dl code, see spotify_dl/youtube.py file
        query = f"{artist} - {song}".replace(":", "").replace("\"", "")
        outtmpl = str(DATA_DIR_TEMP_PATH / f"{song}.%(ext)s")
        ydl_opts = {
            'format': Y_DL_FORMAT_STRING,
            'outtmpl': outtmpl,
            'default_search': 'ytsearch',
            'noplaylist': True,
            'postprocessor_args': ['-metadata', 'title=' + song,
                                   '-metadata', 'artist=' + artist]
        }
        mp3_postprocess_opts = {
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }
        ydl_opts['postprocessors'] = [mp3_postprocess_opts.copy()]

        song_download_successful = False
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            for attempt in range(1, 1 + MAX_Y_DL_RETRIALS):
                try:
                    ydl.download([query])
                    song_download_successful = True
                    break
                except Exception as e:
                    LOGGER.warning(f"YouTube download attempt #{attempt} for episode '{episode.title}' and "
                                   f"song '{song}' failed: {e}")
                    if attempt == MAX_Y_DL_RETRIALS:
                        LOGGER.warning("Giving up!")
                        continue
        if not song_download_successful:
            episode_download_successful = False

    if not episode_download_successful:
        return

    # merge scene files if necessary and move to final location
    if len(episode.songs) > 1:
        segments = []
        for song in episode.songs:
            segment = AudioSegment.from_mp3(str(DATA_DIR_TEMP_PATH / f"{song}.mp3"))
            segments.append(segment)
        final_audio_file = segments[0]
        for segment in segments[1:]:
            final_audio_file += segment
        final_audio_file.export(episode.final_path, format="mp3",
                                tags={"title": episode.title, "artist": "Philip Maloney"})

        # delete scenes
        for song in episode.songs:
            os.remove(DATA_DIR_TEMP_PATH / f"{song}.mp3")
    else:
        episode.move_from_temp_to_final()


def download_episode_from_drs3(episode: Drs3Episode) -> bool:
    ydl_opts = {
        'outtmpl': str(episode.temp_path),
        'postprocessor_args': ['-metadata', 'title=' + episode.title,
                               '-metadata', 'artist=Philip Maloney']
    }

    download_successful = False
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        for attempt in range(1, 1 + MAX_Y_DL_RETRIALS):
            try:
                ydl.download([episode.download_url])
                download_successful = True
                break
            except Exception as e:
                LOGGER.warning(f"DRS3-download attempt #{attempt} of episode '{episode.title}' failed: {e}")
                if attempt == MAX_Y_DL_RETRIALS:
                    LOGGER.warning("Giving up!")

    if download_successful:
        assert episode.temp_path.is_file(), f"Episode '{episode.title}' was downloaded with Y-DL from DRS3, but " \
                                            f"the file is missing!"
        return True

    return False


def is_episode_already_downloaded(episode: Episode) -> bool:
    return episode.final_path.is_file()


def is_episode_known_as_duplicate(episode: Episode) -> Optional[str]:
    if not DUPLICATE_LIST_FILE.is_file():
        return None

    duplicates: Dict[str, str] = json.loads(DUPLICATE_LIST_FILE.read_text())
    return duplicates.get(episode.title, None)


def register_duplicate(duplicate_name: str, episode_name: str) -> None:
    duplicates: Dict[str, str]
    if not DUPLICATE_LIST_FILE.is_file():
        duplicates = {}
    else:
        duplicates = json.loads(DUPLICATE_LIST_FILE.read_text())

    duplicates[duplicate_name] = episode_name

    with DUPLICATE_LIST_FILE.open(mode="wt") as f:
        json.dump(duplicates, f)


def build_fingerprints_and_check_for_duplicates():
    LOGGER.info("Checking for duplicates, this may take an hour or longer")
    for episode_file in DATA_DIR_PATH.glob("*.mp3"):
        episode = Episode(title=episode_file.name.rstrip(".mp3"))
        potentially_existing_episode_name = is_episode_already_known_as_duplicate(episode)
        if not potentially_existing_episode_name:
            add_to_fingerprint_db(episode)
        elif potentially_existing_episode_name != episode.title:
            register_duplicate(duplicate_name=episode.title, episode_name=potentially_existing_episode_name)
            LOGGER.warning(f"YouTube-downloaded episode '{episode.title}' already exist under different "
                           f"name '{potentially_existing_episode_name}'")


def is_episode_already_fingerprinted(episode: Episode) -> bool:
    with suppress(OSError):
        with open("/root/.olaf/file_list.json", "r") as f:
            scanned_files: Dict[int, str] = json.load(f)  # maps from internal ID to absolute path
            return str(episode.final_path) in scanned_files.values()


def is_episode_already_known_as_duplicate(episode: Episode) -> Optional[str]:
    if is_episode_already_fingerprinted(episode):
        return episode.title

    episode_path = episode.temp_path if episode.temp_path.is_file() else episode.final_path
    complete_segment = AudioSegment.from_mp3(episode_path)
    # After 30 seconds the introduction music has finished
    QUERY_CLIP_LENGTH_MS = 60000
    THIRTY_SECS_MS = 30000
    query_segment = complete_segment[THIRTY_SECS_MS:THIRTY_SECS_MS + QUERY_CLIP_LENGTH_MS]
    query_clip_path = DATA_DIR_TEMP_PATH / "query_clip.mp3"
    query_segment.export(query_clip_path, format="mp3")

    # As per https://github.com/JorenSix/Olaf the "monitor" command takes the query clip, splits it into smaller clips
    # of 5 seconds, and queries them
    output = subprocess.check_output(["olaf", "monitor", str(query_clip_path)])
    output_text = output.decode("utf-8")

    """
    The output of "olaf monitor <path>" is something like this:
    query index,total queries, query name, match name, match id, match count (#), q to ref time delta (s), ref start (s), ref stop (s), query time (s)
    1, 1, extract.mp3, Auf der Flucht.mp3, 4147541459, 63, -199.68, 199.90, 208.32, 8.64
    1, 1, extract.mp3, NO_MATCH, 0, 0, 0.00, 0.00, 0.00, 0.00
    <many more lines similar to the above ones>
    
    We do not need the first and last line, as they contain no meaningful data.
    We only care about the "match name" column (3rd column, 0-indexed)
    
    The general idea of identifying a duplicate is that we determine the majority of identified matches and return it.
    """

    sample_count = len(output_text.splitlines()) - 1  # discard the first line
    if sample_count < 3:
        LOGGER.debug(f"'olaf monitor' command produced only produced {sample_count} sample(s) for "
                     f"episode '{episode.title}', skipping duplicate detection!")
        os.unlink(query_clip_path)
        return None

    matched_episodes = []
    for line in output_text.splitlines(keepends=False)[1:]:  # skip first line
        matched_episode = line.split(", ")[3]
        # Olaf's DB contains the file names (including extension), which we don't care about here
        matched_episode = matched_episode.rstrip(".mp3")
        matched_episodes.append(matched_episode)

    counter = Counter(matched_episodes)
    most_common_episode = counter.most_common(1)[0]
    most_common_episode_name, most_common_episode_count = most_common_episode

    found_clear_winner = (most_common_episode_count / sample_count) > 0.6

    if found_clear_winner:
        return_val = None if most_common_episode_name == "NO_MATCH" else most_common_episode_name
    else:
        LOGGER.debug(f"Unable to clearly identify duplicate for episode '{episode.title}'. Most common "
                     f"matches: {counter.most_common(3)} - with a total of {sample_count} samples")
        return_val = None

    os.unlink(query_clip_path)

    return return_val


def add_to_fingerprint_db(episode: Episode):
    assert episode.final_path.is_file(), f"Cannot add episode '{episode.title}' to fingerprint DB, file is missing!"
    output = subprocess.check_output(["olaf", "store", str(episode.final_path)])
    output_text = output.decode("utf-8")
    lines = output_text.splitlines()
    assert "times realtime" in lines[0], f"Unexpected output of Olaf store command: {output_text}"
