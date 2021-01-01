"""
To download old episodes from YouTube, we need a list of playlist IDs, as shown on
https://www.youtube.com/channel/UCfUBvjRrSvAwanMNA5bGB8Q/playlists?view=50&sort=dd&shelf_id=17666223384013636040

Requirement: pip install beautifulsoup4
"""
from bs4 import BeautifulSoup
from pathlib import Path
import re

QUALIFIED_CLASS = ['yt-simple-endpoint', 'style-scope', 'ytd-playlist-thumbnail']

if __name__ == '__main__':
    playlist_ids = []
    html_file = Path(__file__).parent / "playlists-on-youtube.html"
    html_markup = html_file.read_text(encoding="utf-8")
    parsed = BeautifulSoup(html_markup, 'html.parser')
    for a_tag in parsed.find_all('a'):
        if a_tag["class"] == QUALIFIED_CLASS and "list=" in a_tag["href"]:
            href: str = a_tag["href"]  # example: '/watch?v=rJHbVUIqMbw&list=OLAK5uy_lLV6d8kFaKyo9tlY8G1MNROJkuuDTYgyw'
            playlist_id = href.split('=')[-1]
            title: str = a_tag.parent.parent.h3.a.string
            title = re.sub('[ \t\n]+', ' ', title)
            playlist_ids.append((title, playlist_id))

    playlist_ids = [(title, id) for title, id in playlist_ids if "Vol." in title]
    playlist_ids.sort(key=lambda x: int(x[0].split(' ')[-1]))
    """
    Note: the resulting list was cleaned and populated with missing CDs/volumes manually, see youtube_playlist.py
    """

