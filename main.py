import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

from typing import List

import requests

from utils import extract_episodes_from_youtube_videos, is_episode_already_downloaded, download_episode_from_yt, \
    download_episode_from_drs3, is_episode_known_as_duplicate, is_episode_already_known_as_duplicate, \
    register_duplicate, add_to_fingerprint_db, build_fingerprints_and_check_for_duplicates, Episode, \
    get_youtube_videos_from_playlists, format_time

LOGGER = logging.getLogger("MaloneyDownloader")
LOGGER.level = logging.DEBUG


def download_old_episodes_from_youtube():
    youtube_videos = get_youtube_videos_from_playlists()
    episodes = extract_episodes_from_youtube_videos(youtube_videos)

    LOGGER.info(f"Fetched {len(youtube_videos)} individual tracks that make up {len(episodes)} episodes")

    for index, episode in enumerate(episodes):
        LOGGER.debug(f"Downloading episode {index + 1}/{len(episodes)}: {episode.title} "
                     f"({len(episode.download_urls)} parts) with a "
                     f"duration of {format_time(episode.duration_in_seconds)}")
        if is_episode_already_downloaded(episode):
            LOGGER.debug(f"Skipping download of episode '{episode.title}' because it is already downloaded")
            continue
        download_episode_from_yt(episode)


def get_drs3_episode_list() -> List[Episode]:
    episodes = []
    url = "https://www.srf.ch/play/radio/show/93a35193-66b6-4426-b7c1-9658cc497124/latestEpisodes?maxDate=ALL"
    for i in range(50):
        response = requests.get(url)
        json_data: dict = response.json()

        current_page_episodes = json_data.get("episodes", None)
        if not current_page_episodes:
            break

        for episode in current_page_episodes:
            title = episode["title"]
            download_url = episode["absoluteDetailUrl"]
            episodes.append(Episode(title=title, download_urls=[download_url]))

        next_page_url = json_data.get("nextPageUrl", "")
        if not next_page_url:
            break

        if next_page_url.startswith('/'):
            next_page_url = "https://www.srf.ch" + next_page_url

        url = next_page_url

    return episodes


def download_new_radio_episodes() -> None:
    """
    Downloads all those Maloney episodes from DRS3's website which are not already downloaded.
    Checks duplicates using the episodes name (early reject) and after downloading (via audio fingerprinting),
    populating the fingerprint DB as more episodes are downloaded.
    """
    episodes = get_drs3_episode_list()
    for index, episode in enumerate(episodes):
        LOGGER.info(f"Processing episode {index + 1}/{len(episodes)}: {episode.title}")

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

        known_episode_name = is_episode_already_known_as_duplicate(episode)
        if known_episode_name:
            LOGGER.warning(f"DRS3 episode '{episode.title}' already exist under different name '{known_episode_name}'")
            register_duplicate(duplicate_name=episode.title, episode_name=known_episode_name)
        else:
            episode.move_from_temp_to_final()
            add_to_fingerprint_db(episode)


if __name__ == '__main__':
    download_old_episodes_from_youtube()
    build_fingerprints_and_check_for_duplicates()
    download_new_radio_episodes()
