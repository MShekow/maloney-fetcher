import logging
from typing import List

import requests
import spotipy
from spotify_dl.scaffold import log, check_for_tokens
from spotify_dl.spotify import fetch_tracks, parse_spotify_url, validate_spotify_url, get_item_name
from spotipy.oauth2 import SpotifyClientCredentials

from utils import extract_episodes_from_raw_songs, is_episode_already_downloaded, download_episode_from_yt, \
    SpotifyEpisode, \
    Drs3Episode, download_episode_from_drs3

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger("MaloneyDownloader")


def download_old_episodes_from_spotify_and_yt():
    if not check_for_tokens():
        exit(1)

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())
    # url = "https://open.spotify.com/playlist/6U9szH2PXofr4xcbkJuteK"
    url = "https://open.spotify.com/album/7iRfCugT5qF56Gq5eVur0U"
    item_type, item_id = parse_spotify_url(url)

    songs = fetch_tracks(sp, item_type, url)
    episodes = extract_episodes_from_raw_songs(raw_songs=songs)
    LOGGER.info(f"Fetched {len(songs)} individual tracks that make up {len(episodes)} episodes")

    for episode in episodes:
        if is_episode_already_downloaded(episode):
            LOGGER.debug(f"Skipping download of episode '{episode.title}' because it is already downloaded")
            continue
        download_episode_from_yt(episode)


def get_drs3_episode_list() -> List[Drs3Episode]:
    episodes = []
    url = "https://www.srf.ch/play/radio/show/93a35193-66b6-4426-b7c1-9658cc497124/latestEpisodes?maxDate=ALL"
    for i in range(200):
        response = requests.get(url)
        json_data: dict = response.json()

        episodes = json_data.get("episodes", default=None)
        if not episodes:
            break

        for episode in episodes:
            title = episode["title"]
            download_url = episode["absoluteDetailUrl"]
            episodes.append(Drs3Episode(title, download_url))

        next_page_url = json_data.get("nextPageUrl", default="")
        if not next_page_url:
            break
        url = next_page_url

    return episodes


def download_new_radio_episodes():
    """
    Get dict that maps from episode name to youtube-dl URL (popup player URL)
    Download episodes with unknown title to temporary location
    perform fingerprint, check if it already exists - if it does not, add it to the fingerprint DB and move it to the
    final destination
    :return:
    """
    for episode in get_drs3_episode_list():
        if is_episode_already_downloaded(episode):
            LOGGER.info(f"Skipping DL of DRS episode '{episode.title}' because it is already downloaded")
            continue

        # SpotifyEpisode is not yet known by its name - but it might be a duplicate!
        # Thus, we download it to temporary storage, first
        download_episode_from_drs3(episode)

        known_episode_name = is_fingerprint_already_known_as(episode)
        if known_episode_name:
            LOGGER.warning(f"Episode '{episode.title}' already exist under different name '{known_episode_name}'")
        else:
            move_to_final_destination(episode)
            add_to_fingerprint_db(episode)


if __name__ == '__main__':
    # download_old_episodes_from_spotify_and_yt()
    download_new_radio_episodes()
