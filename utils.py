import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

import youtube_dl
from pydub import AudioSegment

DATA_DIR_PATH = Path("/data")
DATA_DIR_TEMP_PATH = DATA_DIR_PATH / "temp"

Y_DL_FORMAT_STRING = "bestaudio/best"

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


@dataclass
class SpotifyEpisode(Episode):
    songs: Dict[str, str]


@dataclass()
class Drs3Episode(Episode):
    download_url: str


def extract_episodes_from_raw_songs(raw_songs: Dict[str, str]) -> List[SpotifyEpisode]:
    episodes: List[SpotifyEpisode] = []
    current_episode_title = ""
    current_episode: Optional[SpotifyEpisode] = None

    for raw_song_title, raw_song_artists in raw_songs.values():
        if ':' not in raw_song_title:
            # some episodes are not split into multiple tracks/scenes
            episode_title = raw_song_title
        else:
            episode_title, _scene_index = raw_song_title.split(':')
        if episode_title != current_episode_title:
            if current_episode:
                episodes.append(current_episode)
            current_episode = SpotifyEpisode(episode_title, {})

        current_episode.songs[raw_song_title] = raw_song_artists

    return episodes


def download_episode_from_yt(episode: SpotifyEpisode):
    """
    Downloads all (parts / "scenes") of the episodes, merges them (if necessary) and stores them in the data directory.
    Retries the download in case a connection error occurred
    """
    for song, artist in episode.songs.items():
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

        # TODO: retrial logic
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            try:
                ydl.download([query])
            except Exception as e:
                # log.debug(e)
                # TODO use logger, clean up previously loaded scenes if they exist
                print('Failed to download: {}, please ensure YouTubeDL is up-to-date. '.format(query))
                continue

        # merge scene files if necessary and move to final location
        if len(episode.songs) > 1:
            segments = []
            for song in episode.songs:
                segment = AudioSegment.from_mp3(str(DATA_DIR_TEMP_PATH / f"{song}.mp3"))
                segments.append(segment)
            final_audio_file = segments[0]
            for segment in segments[1:]:
                final_audio_file += segment
            final_audio_file.export(DATA_DIR_PATH / f"{episode.title}.mp3", format="mp3",
                                    tags={"title": episode.title, "artist": "Philip Maloney"})

            # delete scenes
            for song in episode.songs:
                os.remove(DATA_DIR_TEMP_PATH / f"{song}.mp3")
        else:
            os.rename(src=episode.temp_path, dst=episode.final_path)


def download_episode_from_drs3(episode: Drs3Episode):
    ydl_opts = {
        'outtmpl': episode.temp_path,
        'postprocessor_args': ['-metadata', 'title=' + episode.title,
                               '-metadata', 'artist=Philip Maloney']
    }

    # TODO: retrial logic
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        try:
            ydl.download([episode.download_url])
        except Exception as e:
            pass
            # TODO use logger, clean up previously loaded scenes if they exist


def is_episode_already_downloaded(episode: Episode) -> bool:
    return episode.final_path.is_file()


def build_fingerprints_and_check_for_duplicates():
    print("Checking for duplicates, this may take an hour or longer")
    try:
        with open("/data/deduplication-report.txt", "wt") as f:
            subprocess.check_call(["olaf", "dedupm", "/data"], stdout=f, stderr=f)
    except subprocess.CalledProcessError:
        LOGGER.exception("Checking for duplicates failed")

def is_fingerprint_already_known_as(episode: Episode) -> Optional[str]:
    # TODO build 3 clips each 10 seconds long, scan them, then delete them
    complete_segment = AudioSegment.from_mp3(episode.temp_path)
    CLIP_LENGTH_SECS = 10
    for i in range(3):
        base_start = i * (complete_segment.duration_seconds / 3)
        start_sec = base_start + (complete_segment.duration_seconds / 6) - (CLIP_LENGTH_SECS / 2)
        end_sec = base_start + (complete_segment.duration_seconds / 6) + (CLIP_LENGTH_SECS / 2)
    output = subprocess.check_output(["olaf", "query", "snippet"])

    """
    Match response:
    query index,total queries, query name, match name, match id, match count (#), q to ref time delta (s), ref start (s), ref stop (s), query time (s)
    1, 1, extract.mp3, Auf der Flucht.mp3, 4147541459, 63, -199.68, 199.90, 208.32, 8.64
    Proccessed 374 fp's from 10.1s in 0.018s (572 times realtime) 


    olTODO no-match response
    query index,total queries, query name, match name, match id, match count (#), q to ref time delta (s), ref start (s), ref stop (s), query time (s)
    1, 1, extract2.mp3, NO_MATCH, 0, 0, 0.00, 0.00, 0.00, 0.00
    Proccessed 607 fp's from 14.8s in 0.020s (741 times realtime)
    
    Or we could simply create a 1 minute clip starting at 00:30 and use olaf monitor - it should be full of NO_MATCH outputs
    Just build a majority
    """
