FROM python:3.8

RUN apt-get update && apt-get install -y sox libsox-fmt-mp3 ffmpeg ruby sudo
RUN pip install youtube_dl pydub requests

RUN git clone https://github.com/JorenSix/Olaf.git && cd Olaf && make && make install
# Olaf also needs this gem for some of its commands
RUN gem install threach

WORKDIR /app
COPY main.py .
COPY utils.py .
COPY youtube_playlists.py .

CMD ["python", "main.py"]
# Use this just for testing
# CMD ["bash"]