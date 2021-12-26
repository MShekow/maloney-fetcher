import logging
from datetime import timezone
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date
from dateutil.utils import today

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

from typing import List

import requests

from utils import extract_episodes_from_youtube_videos, is_episode_already_downloaded, download_episode_from_yt, \
    download_episode_from_drs3, is_episode_known_as_duplicate, is_episode_already_known_as_duplicate, \
    register_duplicate, add_to_fingerprint_db, Episode, \
    get_youtube_videos_from_playlists, format_time, build_fingerprints_and_check_for_duplicates

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
    episodes: List[Episode] = []
    LIMIT = 20
    episode_list_url_template = f"https://www.srf.ch/audio/episodes/10000183/{LIMIT}/{{offset}}"
    current_offset = 0
    while True:
        url = episode_list_url_template.format(offset=current_offset)
        response = requests.get(url).json()
        parsed = BeautifulSoup(response["content"], 'html.parser')
        episodes_found_on_page = []
        for div in parsed.contents:
            title = div.find("div", class_="media-caption__title").string.strip()
            episode = Episode(title=title, download_urls=[])
            if is_episode_already_downloaded(episode):
                LOGGER.debug(f"Skipping download of episode '{title}' because it is already downloaded")
                continue

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


def download_new_radio_episodes() -> None:
    """
    Downloads all those Maloney episodes from DRS3's website which are not already downloaded.
    Checks duplicates using the episodes name (early reject) and after downloading (via audio fingerprinting),
    populating the fingerprint DB as more episodes are downloaded.
    """
    LOGGER.info("Retrieving episode list from DRS3 (may take a minute)")
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
