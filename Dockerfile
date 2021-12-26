FROM python:3.8

RUN apt-get update && apt-get install -y sox libsox-fmt-mp3 ffmpeg ruby sudo
RUN pip install yt-dlp pydub requests lxml bs4 python-dateutil eyeD3

RUN git clone https://github.com/JorenSix/Olaf.git \
    && cd Olaf \
    && git checkout bbdaef2267f82aac78fdd422506f0bba1613dfad \
    && make && make install

# Olaf also needs this gem for some of its commands
RUN gem install threach

WORKDIR /app
COPY main.py .
COPY utils.py .
COPY youtube_playlists.py .

CMD ["python", "main.py"]
# Use this just for testing
# CMD ["bash"]