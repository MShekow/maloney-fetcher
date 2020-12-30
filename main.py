import logging
from typing import List

import requests
import spotipy
from spotify_dl.scaffold import check_for_tokens
from spotify_dl.spotify import fetch_tracks, parse_spotify_url
from spotipy.oauth2 import SpotifyClientCredentials

from utils import extract_episodes_from_raw_songs, is_episode_already_downloaded, download_episode_from_yt, \
    Drs3Episode, download_episode_from_drs3, is_episode_known_as_duplicate, is_fingerprint_already_known_as, \
    register_duplicate, add_to_fingerprint_db

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger("MaloneyDownloader")


def download_old_episodes_from_spotify_and_yt():
    if not check_for_tokens():
        exit(1)

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())
    # url = "https://open.spotify.com/playlist/6U9szH2PXofr4xcbkJuteK"  # this is the real playlist!
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


def download_new_radio_episodes() -> None:
    """
    Downloads all those Maloney episodes from DRS3's website which are not already downloaded.
    Checks duplicates using the episodes name (early reject) and after downloading (via audio fingerprinting),
    populating the fingerprint DB as more episodes are downloaded.
    """
    for episode in get_drs3_episode_list():
        if is_episode_already_downloaded(episode):
            LOGGER.info(f"Skipping DL of DRS episode '{episode.title}' because it is already downloaded")
            continue

        # Check whether this episode's title was already previously identified (and registered) as duplicate
        real_episode_name = is_episode_known_as_duplicate(episode)
        if real_episode_name:
            LOGGER.info(f"Skipping DL of DRS episode '{episode.title}' because it is a duplicate - real "
                        f"episode title: '{real_episode_name}'")
            continue

        # Episode is not yet known by its name - but it might be a duplicate!
        # Thus, we download it to temporary storage, first
        success = download_episode_from_drs3(episode)

        if not success:
            continue

        known_episode_name = is_fingerprint_already_known_as(episode)
        if known_episode_name:
            LOGGER.warning(f"DRS3 episode '{episode.title}' already exist under different name '{known_episode_name}'")
            register_duplicate(duplicate_name=episode.title, episode_name=known_episode_name)
        else:
            episode.move_from_temp_to_final()
            add_to_fingerprint_db(episode)


if __name__ == '__main__':
    download_old_episodes_from_spotify_and_yt()
    download_new_radio_episodes()
