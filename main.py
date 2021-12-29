import logging
from typing import List

from utils import extract_episodes_from_youtube_videos, register_duplicate, add_to_fingerprint_db, Episode, \
    get_youtube_videos_from_playlists, check_is_episode_known, SearchMode, get_drs3_episode_list, \
    is_episode_already_fingerprinted

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger("MaloneyDownloader")
LOGGER.level = logging.DEBUG

if __name__ == '__main__':
    LOGGER.info("Retrieving episode list from YouTube - this will take approx. 2 minutes")
    youtube_videos = get_youtube_videos_from_playlists()
    youtube_episodes = extract_episodes_from_youtube_videos(youtube_videos)
    LOGGER.info(f"Fetched {len(youtube_videos)} individual tracks that make "
                f"up {len(youtube_episodes)} YouTube episodes")

    LOGGER.info("Retrieving episode list from DRS3 (may take a minute)")
    drs3_episodes = get_drs3_episode_list()

    episodes: List[Episode] = youtube_episodes + drs3_episodes

    LOGGER.info(f"Fetched a total of {len(episodes)} episodes. Beginning download...")

    for index, episode in enumerate(episodes):
        LOGGER.info(f"Processing episode #{index + 1}/{len(episodes)}: {episode}")
        known_episode_title = check_is_episode_known(episode, search_mode=SearchMode.CacheOnly)
        if known_episode_title:
            LOGGER.info(f"Skipping DL of {episode} because it is already downloaded as '{known_episode_title}'")
            if not is_episode_already_fingerprinted(episode) and known_episode_title == episode.title:
                known_episode_title_fingerprint = check_is_episode_known(episode, search_mode=SearchMode.Fingerprinting)
                if known_episode_title_fingerprint:
                    LOGGER.warning(f"{episode} already exist under different name '{known_episode_title_fingerprint}'")
                    register_duplicate(duplicate_name=episode.title, episode_name=known_episode_title)
                else:
                    add_to_fingerprint_db(episode)
            continue

        success = episode.download_to_temp_file()
        if not success:
            LOGGER.warning(f"Unable to download {episode}")
            continue

        known_episode_title = check_is_episode_known(episode, search_mode=SearchMode.Fingerprinting)
        if known_episode_title:
            LOGGER.warning(f"{episode} already exist under different name '{known_episode_title}'")
            register_duplicate(duplicate_name=episode.title, episode_name=known_episode_title)
        else:
            episode.move_from_temp_to_final()
            add_to_fingerprint_db(episode)
