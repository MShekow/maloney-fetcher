import json
import logging
import os
import subprocess
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import youtube_dl
from pydub import AudioSegment

from youtube_playlists import YOUTUBE_PLAYLIST_IDS

DATA_DIR_PATH = Path("/data")
DATA_DIR_TEMP_PATH = DATA_DIR_PATH / "temp"
DUPLICATE_LIST_FILE = DATA_DIR_PATH / "duplicates.csv"

MAX_Y_DL_RETRIALS = 3
TWELVE_MINUTES = 12 * 60
THIRTY_FIVE_MINUTES = 35 * 60

LOGGER = logging.getLogger("MaloneyDownloader")


class YouTubeDlHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.messages = []

    def emit(self, record: logging.LogRecord):
        self.messages.append(record.getMessage())


@dataclass
class Episode:
    title: str
    download_urls: List[str]

    @property
    def final_path(self) -> Path:
        return DATA_DIR_PATH / f"{self.title}.mp3"

    @property
    def temp_path(self) -> Path:
        return DATA_DIR_TEMP_PATH / f"{self.title}.mp3"

    def move_from_temp_to_final(self):
        os.rename(src=self.temp_path, dst=self.final_path)


@dataclass
class YouTubeEpisode(Episode):
    duration_in_seconds: int


@dataclass
class YouTubeVideo:
    title: str
    duration_in_seconds: int
    video_id: str


def format_time(seconds: int) -> str:
    def get_formatted_int(val: int):
        return "%02d" % (val,)

    seconds = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + seconds

    if d.day - 1 != 0:
        return "%d days %s:%s:%s" % (
            d.day - 1, get_formatted_int(d.hour), get_formatted_int(d.minute), get_formatted_int(d.second))
    elif d.hour != 0:
        return "%s:%s:%s hours" % (
            get_formatted_int(d.hour), get_formatted_int(d.minute), get_formatted_int(d.second))
    elif d.minute != 0:
        return "%s:%s min" % (get_formatted_int(d.minute), get_formatted_int(d.second))
    else:
        return "%d sec" % d.second


def get_youtube_videos_from_playlists() -> List[YouTubeVideo]:
    LOGGER.info("Retrieving episode list of YouTube - this will take approx. 5 minutes")
    videos = []

    helper_logger = logging.getLogger("YouTube-DL Helper Logger")
    helper_logger.level = logging.DEBUG
    helper_logger.propagate = False

    for title, playlist_id in YOUTUBE_PLAYLIST_IDS:
        handler = YouTubeDlHandler()
        helper_logger.handlers = [handler]

        query = f"https://www.youtube.com/playlist?list={playlist_id}"
        ydl_opts = {
            'dump_single_json': True,
            'extract_flat': True,
            'logger': helper_logger,
        }

        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            for attempt in range(1, 1 + MAX_Y_DL_RETRIALS):
                try:
                    ydl.download([query])
                    ydl_log_output = handler.messages
                    # Expect that the last line contains the single JSON dump that contains all information
                    last_output_line = ydl_log_output[-1]
                    assert last_output_line.startswith('{'), f"Last output line of Y-DL should be JSON, " \
                                                             f"but is {ydl_log_output[-1]}"
                    json_data = json.loads(last_output_line)
                    entry: dict
                    for entry in json_data["entries"]:
                        videos.append(YouTubeVideo(title=entry["title"], duration_in_seconds=int(entry["duration"]),
                                                   video_id=entry["id"]))
                    break
                except Exception as e:
                    LOGGER.warning(f"Attempt #{attempt} failed to get videos in playlist: {e}")
                    if attempt == MAX_Y_DL_RETRIALS:
                        LOGGER.warning("Giving up!")
                        continue

    return videos


def extract_episodes_from_youtube_videos(videos: List[YouTubeVideo]) -> List[YouTubeEpisode]:
    episodes: List[YouTubeEpisode] = []
    current_episode_title = ""
    current_episode: Optional[YouTubeEpisode] = None

    for video in videos:
        if ':' not in video.title:
            # some episodes are not split into multiple tracks/scenes
            episode_title = video.title
        else:
            episode_title, _scene_index = video.title.split(':')
        if episode_title != current_episode_title:
            if current_episode:
                episodes.append(current_episode)
            current_episode = YouTubeEpisode(title=episode_title, download_urls=[], duration_in_seconds=0)
            current_episode_title = current_episode.title

        current_episode.duration_in_seconds += video.duration_in_seconds
        current_episode.download_urls.append(f"https://www.youtube.com/watch?v={video.video_id}")

    if current_episode and current_episode not in episodes:
        episodes.append(current_episode)

    # Perform a sanity check on episode length
    for episode in episodes:
        if episode.duration_in_seconds < TWELVE_MINUTES or episode.duration_in_seconds > THIRTY_FIVE_MINUTES:
            LOGGER.warning(f"Episode '{episode.title}' has an unplausible duration "
                           f"of {format_time(episode.duration_in_seconds)}")

    return episodes


def download_episode_from_yt(episode: YouTubeEpisode):
    """
    Downloads all (parts / "scenes") of the episodes, merges them (if necessary) and stores them in the data directory.
    Retries the download in case a connection error occurred.
    """

    def get_temp_file_name_for_episode_part(download_url_index: int) -> str:
        if len(episode.download_urls) == 1:
            return episode.title
        return f"{episode.title}_{download_url_index}"

    episode_download_successful = True
    for index, download_url in enumerate(episode.download_urls):
        # Building the ydl_opts dict was taken from the spotify-dl code, see spotify_dl/youtube.py file
        outtmpl = str(DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.%(ext)s")
        ydl_opts = {
            'format': "bestaudio/best",
            'outtmpl': outtmpl,
            'default_search': 'ytsearch',
            'noplaylist': True,
            'quiet': True,
            'postprocessor_args': ['-metadata', 'title=' + episode.title,
                                   '-metadata', 'artist=Philip Maloney']
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
                    ydl.download([download_url])
                    # perform a sanity check, just in case - sometimes the DL fails silently for
                    # no good reason, no idea why
                    temp_path = DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.mp3"
                    assert temp_path.is_file(), f"Download of index {index} for episode '{episode.title}' " \
                                                f"failed: MP3 file is missing!"
                    song_download_successful = True
                    break
                except Exception as e:
                    LOGGER.warning(f"YouTube download attempt #{attempt} for episode '{episode.title}' and "
                                   f"download URL '{download_url}' (index {index}) failed: {e}")
                    if attempt == MAX_Y_DL_RETRIALS:
                        LOGGER.warning("Giving up!")
                        continue
        if not song_download_successful:
            episode_download_successful = False

    if not episode_download_successful:
        LOGGER.warning("Aborting the merging of the part files, because at least one part could not be downloaded")
        return

    # merge scene files if necessary and move to final location
    if len(episode.download_urls) > 1:
        segments = []
        for index, download_url in enumerate(episode.download_urls):
            segment = AudioSegment.from_mp3(
                str(DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.mp3"))
            segments.append(segment)
        final_audio_file = segments[0]
        for segment in segments[1:]:
            final_audio_file += segment
        final_audio_file.export(episode.final_path, format="mp3",
                                tags={"title": episode.title, "artist": "Philip Maloney"})

        # Sanity check
        duration_difference = abs(final_audio_file.duration_seconds - episode.duration_in_seconds)
        if duration_difference > 15:
            LOGGER.warning(f"Final audio file has a duration difference of {format_time(duration_difference)}, "
                           f"something is wrong!")

        # delete scenes
        for index in range(len(episode.download_urls)):
            os.remove(DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.mp3")
    else:
        episode.move_from_temp_to_final()


def download_episode_from_drs3(episode: Episode) -> bool:
    ydl_opts = {
        'outtmpl': str(episode.temp_path),
        'quiet': True,
        'postprocessor_args': ['-metadata', 'title=' + episode.title,
                               '-metadata', 'artist=Philip Maloney']
    }

    download_successful = False
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        for attempt in range(1, 1 + MAX_Y_DL_RETRIALS):
            try:
                ydl.download([episode.download_urls[0]])
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
        episode = Episode(title=episode_file.name.rstrip(".mp3"), download_urls=[])
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
    
    We do not need the first line, as it contains no meaningful data.
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
