# Maloney Fetcher

This is a quickly hacked project that fetches episodes from the (Swiss) German radio play 
"Die haarsträubenden Fälle des Philip Maloney" ([website](https://www.srf.ch/audio/maloney)).

You can use it obtain all episodes that were ever aired on DRS3 (radio station) or produced on CD.

Please buy the CDs if you like the show. This project exist primarily for research purposes, testing audio fingerprinting algorithms.
The goal of this project is to determine whether the show authors (or radio managers) are airing the same episodes
under different names. This hypothesis arises from the fact that only ca. 400 distinct episodes seem to exist, but the
show has been on air since over 30 years ([German source](https://www.tagblatt.ch/kultur/unsterbliche-sache-maloney-ld.1114780)),
being aired weekly, which means that there are `30 * 52 = 1560` broadcasts, with much fewer episodes.

## Requirements

- Docker (or something that supports `Dockerfile`, e.g. Podman+buildah) to build the image and run the software

## Dependencies
- [Olaf](https://github.com/JorenSix/Olaf) for audio fingerprinting (detecting episode duplicates)
- [YouTube-dl](https://github.com/ytdl-org/youtube-dl) to download the actual episodes from YouTube (or DRS3), using the meta-data scraped from YouTube or the DRS3 website
- [requests](https://github.com/psf/requests/) to scrape DRS3's convenient API for episode meta-data
- [pydub](https://github.com/jiaaro/pydub) to merge episode mp3 files which are split into multiple scenes (CD tracks) on YouTube


## Usage

Inside the container, the code stores episode mp3 files in `/data` and the fingerprint database (which uses [Olaf](https://github.com/JorenSix/Olaf))
is stored in `/root/.olaf`.

To build the software, just run `docker build -t maloney-fetcher .`

Here is an example how to run it:
`docker run -v D:/Maloney:/data -v D:/Maloney/fingerprintdb:/root/.olaf maloney-fetcher`

If you are on Unix or macOS and are not using Docker rootless or Podman, you should also provide the `--user $(whoami)` argument to fix permissions.