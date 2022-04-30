import json
import logging
import os
import subprocess
import sys
from abc import abstractmethod
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs
from urllib.request import urlretrieve

import eyed3
import requests
import yt_dlp
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date
from dateutil.utils import today
from pydub import AudioSegment

from youtube_playlists import YOUTUBE_PLAYLIST_IDS

DATA_DIR_PATH = Path("/data")
DATA_DIR_TEMP_PATH = DATA_DIR_PATH / "temp"
DUPLICATE_LIST_FILE = DATA_DIR_PATH / "duplicates.csv"

MAX_Y_DL_RETRIALS = 3
TWELVE_MINUTES = 12 * 60
THIRTY_FIVE_MINUTES = 35 * 60

LOGGER = logging.getLogger("MaloneyDownloader")
LOGGER.level = logging.DEBUG


class SearchMode(Enum):
    CacheOnly = 1
    Fingerprinting = 2


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

    @abstractmethod
    def download_to_temp_file(self) -> bool:
        """
        Downloads the episode to the temp_path, and returns True if the download was successful, False otherwise.
        """

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

    def download_to_temp_file(self) -> bool:
        """
        Downloads all (parts / "scenes") of the episodes, merges them (if necessary) and stores them in the temporary
        directory. Retries the download in case a connection error occurred.
        """

        def get_temp_file_name_for_episode_part(download_url_index: int) -> str:
            if len(self.download_urls) == 1:
                return self.title
            return f"{self.title}_{download_url_index}"

        episode_download_successful = True
        for index, download_url in enumerate(self.download_urls):
            # Building the ydl_opts dict was taken from the spotify-dl code, see spotify_dl/youtube.py file
            outtmpl = str(DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.%(ext)s")
            ydl_opts = {
                'format': "bestaudio",
                'outtmpl': outtmpl,
                'default_search': 'ytsearch',
                'noplaylist': True,
                'quiet': True,
                'noprogress': True
            }
            mp3_postprocess_opts = {
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }
            # YDL might download the clip as some kind of video (e.g. webm), from which we need the audio as MP3
            ydl_opts['postprocessors'] = [mp3_postprocess_opts.copy()]

            scene_download_successful = False
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                for attempt in range(1, 1 + MAX_Y_DL_RETRIALS):
                    try:
                        ydl.download([download_url])
                        # perform a sanity check, just in case - sometimes the DL fails silently for
                        # no good reason, no idea why
                        temp_path = DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.mp3"
                        assert temp_path.is_file(), f"Download of index {index} for episode '{self.title}' " \
                                                    f"failed: MP3 file is missing!"
                        scene_download_successful = True
                        break
                    except Exception as e:
                        LOGGER.warning(f"YouTube download attempt #{attempt} for episode '{self.title}' and "
                                       f"download URL '{download_url}' (index {index}) failed: {e}")
                        if attempt == MAX_Y_DL_RETRIALS:
                            LOGGER.warning("Giving up!")
                            continue
            if not scene_download_successful:
                episode_download_successful = False

        if not episode_download_successful:
            LOGGER.warning("Aborting the merging of the part files, because at least one part could not be downloaded")
            return False

        if len(self.download_urls) > 1:
            # merge scene files if necessary and move to temporary location
            segments = []
            for index, download_url in enumerate(self.download_urls):
                segment = AudioSegment.from_mp3(
                    str(DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.mp3"))
                segments.append(segment)
            merged_audio_file = segments[0]
            for segment in segments[1:]:
                merged_audio_file += segment
            merged_audio_file.export(self.temp_path, format="mp3",
                                     tags={"title": self.title, "artist": "Philip Maloney"})

            # Sanity check
            duration_difference = abs(merged_audio_file.duration_seconds - self.duration_in_seconds)
            if duration_difference > 15:
                LOGGER.warning(f"Merged audio file has a duration difference of {format_time(duration_difference)}, "
                               f"something is wrong!")

            # delete scenes
            for index in range(len(self.download_urls)):
                os.remove(DATA_DIR_TEMP_PATH / f"{get_temp_file_name_for_episode_part(index)}.mp3")
        else:
            # Update only the ID3 tag
            fix_mp3_tags(self.title, str(self.temp_path))

        return True

    def __str__(self):
        return f"YouTube episode '{self.title}'"


@dataclass
class Drs3Episode(Episode):
    def download_to_temp_file(self) -> bool:
        try:
            urlretrieve(self.download_urls[0], self.temp_path)  # performs a streaming download
        except Exception as e:
            LOGGER.warning(f"Unable to download episode {self.title} from {self.download_urls[0]}: {e}")
            return False

        fix_mp3_tags(self.title, str(self.temp_path))

        return True

    def __str__(self):
        return f"DRS3 episode '{self.title}'"


@dataclass
class YouTubeVideo:
    """
    Represents an individual YouTube video (might be just an individual scene of a Maloney episode, or an entire
    episode).
    """
    title: str
    duration_in_seconds: int
    video_id: str


def fix_mp3_tags(episode_title: str, file_path: str):
    audiofile = eyed3.load(file_path)
    if audiofile.tag is None:
        audiofile.initTag()
    audiofile.tag.artist = "Philip Maloney"
    audiofile.tag.title = episode_title
    audiofile.tag.save()


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


class StdoutCapturer:
    """
    Helper class that captures calls to sys.stdout.write(), which is used under the hood by yt_dlp (when dumping the
    JSON containing the individual episodes of a YouTube playlist).
    """

    def __init__(self):
        self.last_written_line = ""
        self._stdout_original = sys.stdout

    def write(self, written_string: str):
        if not isinstance(written_string, str):
            raise RuntimeError(f"Expected a string to be written, but got {written_string!r}")
        self.last_written_line = written_string

    def flush(self):
        """
        Dummy method that does nothing
        """

    def __enter__(self):
        sys.stdout = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._stdout_original
        # Note: by returning nothing, any exceptions that occurred will be automatically re-raised by the Python runtime


def get_youtube_videos_from_playlists() -> List[YouTubeVideo]:
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

        with StdoutCapturer() as capturer:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                for attempt in range(1, 1 + MAX_Y_DL_RETRIALS):
                    try:
                        ydl.download([query])
                        # Expect that the last line contains the single JSON dump that contains all information
                        json_line_dump = capturer.last_written_line
                        assert json_line_dump.startswith('{'), f"Last output line of Y-DL should be JSON, " \
                                                               f"but is {json_line_dump[-1]}"
                        json_data = json.loads(json_line_dump)
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
    def has_implausible_length(episode: YouTubeEpisode) -> bool:
        return episode.duration_in_seconds < TWELVE_MINUTES or episode.duration_in_seconds > THIRTY_FIVE_MINUTES

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
        if has_implausible_length(episode):
            LOGGER.warning(f"Episode '{episode.title}' has an implausible duration "
                           f"of {format_time(episode.duration_in_seconds)}")

    # Actually exclude those that have implausible lengths
    episodes = [e for e in episodes if not has_implausible_length(e)]

    return episodes


def is_episode_already_downloaded(episode: Episode) -> bool:
    return episode.final_path.is_file() and episode.final_path.stat().st_size > 8 * 1024 * 1024


def is_episode_known_as_duplicate(episode: Episode) -> Optional[str]:
    """
    Checks whether the duplication DB already knows the provided episode's title, returning the "real" name.
    Returns None otherwise.
    """
    if not DUPLICATE_LIST_FILE.is_file():
        return None

    duplicates: Dict[str, str] = json.loads(DUPLICATE_LIST_FILE.read_text())
    return duplicates.get(episode.title, None)


def register_duplicate(duplicate_name: str, episode_name: str) -> None:
    """ Adds the duplicate to the duplication DB. """
    duplicates: Dict[str, str]
    if not DUPLICATE_LIST_FILE.is_file():
        duplicates = {}
    else:
        duplicates = json.loads(DUPLICATE_LIST_FILE.read_text())

    duplicates[duplicate_name] = episode_name

    with DUPLICATE_LIST_FILE.open(mode="wt") as f:
        json.dump(duplicates, f)


def is_episode_already_fingerprinted(episode: Episode) -> bool:
    """ Returns True if the episode's absolute path is already known to the fingerprinting DB, False otherwise. """
    with suppress(OSError):
        with open("/root/.olaf/file_list.json", "r") as f:
            scanned_files: Dict[int, str] = json.load(f)  # maps from internal ID to absolute path
            return str(episode.final_path) in scanned_files.values()


def check_is_episode_known(episode: Episode, search_mode: SearchMode) -> Optional[str]:
    """
    Checks whether the episode is already known / downloaded. If so, the name of the episode is returned, which might
    be different from `episode.title` in case fingerprinting revealed that the provided episode is a duplicate.

    If search_mode is SearchMode.CacheOnly, then only locally-cached information is considered, e.g. the presence of
    a file name, or the duplication DB.
    """
    if search_mode is SearchMode.CacheOnly:
        if is_episode_already_downloaded(episode) or is_episode_already_fingerprinted(episode):
            return episode.title
        return is_episode_known_as_duplicate(episode)

    # SearchMode is Fingerprinting
    episode_path = episode.temp_path if episode.temp_path.is_file() else episode.final_path
    if not episode_path.is_file():
        LOGGER.warning(f"File path {episode_path} does not exist")
        return None

    complete_segment = AudioSegment.from_mp3(episode_path)
    # After 30 seconds the introduction music has finished
    QUERY_CLIP_LENGTH_MS = 90000
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

    found_clear_winner = (most_common_episode_count / sample_count) > 0.5

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


def get_drs3_episode_list() -> List[Drs3Episode]:
    episodes: List[Drs3Episode] = []
    LIMIT = 20
    episode_list_url_template = f"https://www.srf.ch/audio/episodes/10000183/{LIMIT}/{{offset}}"
    current_offset = 0
    while True:
        url = episode_list_url_template.format(offset=current_offset)
        response = requests.get(url).json()
        parsed = BeautifulSoup(response["content"], 'html.parser')
        episodes_found_on_page = []
        for div in parsed.contents:
            title = div.find("h4", class_="media-caption__title").string.strip()
            episode = Drs3Episode(title=title, download_urls=[])

            # Grab the episode's details page, to determine it's low-level ID
            relative_page_url = div.find("a", class_="medium__play-link").attrs["href"].lstrip('/')
            absolute_page_url = "https://www.srf.ch/" + relative_page_url
            episode_page_content = requests.get(absolute_page_url).content.decode("utf-8")
            parsed_episode_content = BeautifulSoup(episode_page_content, 'html.parser')
            """
            Look for a div like this, from which we only need to extract the "?id" query parameter:
            <div class="js-media"
                data-app-audio
                tabindex="0"
                data-href="/play/radio/_/audio/_?id=95991277-dcb9-40df-8a35-e3fbeafdca75&urn=urn:srf:audio:95991277-dcb9-40df-8a35-e3fbeafdca75" ....
            </div>
            """
            relative_data_url = parsed_episode_content.find("div", class_="js-media").attrs["data-href"].lstrip('/')
            parsed_url = urlparse("https://www.srf.ch/" + relative_data_url)
            episode_id = parse_qs(parsed_url.query)['id'][0]

            # Retrieve the JSON episode details
            episode_metadata_url_template = "https://il.srgssr.ch/integrationlayer/2.0/mediaComposition/byUrn/urn:srf:audio:{episode_id}.json?onlyChapters=true&vector=portalplay"
            episode_metadata = requests.get(episode_metadata_url_template.format(episode_id=episode_id)).json()
            valid_to_datetime_string = episode_metadata["chapterList"][0]["validTo"]
            valid_to_date = parse_date(valid_to_datetime_string)
            if today().replace(tzinfo=timezone.utc) > valid_to_date:
                LOGGER.info(f"Aborting at episode {title} because its validity expired on {valid_to_date}")
                break

            download_url: str = episode_metadata["chapterList"][0]["resourceList"][0]["url"]
            episode.download_urls = [download_url]
            episodes_found_on_page.append(episode)

        if not episodes_found_on_page:
            break

        episodes.extend(episodes_found_on_page)

        current_offset += LIMIT

    return episodes
