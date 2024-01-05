"""
To download old episodes from YouTube, we need a list of playlist IDs, as shown on
https://www.youtube.com/channel/UCfUBvjRrSvAwanMNA5bGB8Q/playlists?view=50&sort=dd&shelf_id=17666223384013636040
(which is the "Michael Schacht - Topic" page)

Instructions:
1) Open the above link
2) Open the browser developer tools
3) On the page, click on "View all"
4) Save the response content of the requests made to the "browser" endpoint to the file playlists.json (one line per result)
5) Run this script, paste the output into the youtube_playlist.py file
"""
import json
from pathlib import Path
import re
from typing import Tuple

QUALIFIED_CLASS = ['yt-simple-endpoint', 'style-scope', 'ytd-grid-playlist-renderer']


def extract_title_and_playlist_id(item: dict) -> Tuple[str, str]:
    title = item['title']['runs'][0]['text']
    playlist_id = item['playlistId']
    return title, playlist_id


VOLUME_REGEX = re.compile(r' (No\.|Vol\. )(?P<number>\d+)')


def extract_volume_number(title: str) -> int:
    match = VOLUME_REGEX.search(title)
    if match:
        return int(match.group('number'))
    else:
        return 0


if __name__ == '__main__':
    playlist_items: list[Tuple[str, str]] = []  # contains tuples of (title, playlist_id)
    playlist_file = Path(__file__).parent / "playlists.json"
    for line in playlist_file.read_text(encoding="utf-8").splitlines():
        data = json.loads(line)
        for continuation_item in data['onResponseReceivedEndpoints'][0]['appendContinuationItemsAction'][
            'continuationItems']:
            if 'gridRenderer' in continuation_item:
                for item in continuation_item['gridRenderer']['items']:
                    if 'gridPlaylistRenderer' in item:
                        playlist_items.append(extract_title_and_playlist_id(item['gridPlaylistRenderer']))

            elif 'gridPlaylistRenderer' in continuation_item:
                playlist_items.append(extract_title_and_playlist_id(continuation_item['gridPlaylistRenderer']))
            elif 'continuationItemRenderer' in continuation_item:
                pass
            else:
                raise ValueError("Unknown structure")

    # sort by volume number
    playlist_items.sort(key=lambda x: extract_volume_number(x[0]))

    # Filter out duplicates
    playlist_items_unique = []
    playlist_items_unique_vol_numbers = set()
    for title, playlist_id in playlist_items:
        vol_number = extract_volume_number(title)
        if vol_number not in playlist_items_unique_vol_numbers:
            playlist_items_unique_vol_numbers.add(vol_number)
            playlist_items_unique.append((title, playlist_id))

    playlist_items_str = "\n".join(
        [f'(\"{title}\", \"{playlist_id}\"),' for title, playlist_id in playlist_items_unique])

    print(f"YOUTUBE_PLAYLIST_IDS = [\n"
          f"{playlist_items_str}\n"
          f"\n]")
