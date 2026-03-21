FROM python:3.11

RUN apt-get update && apt-get install -y sox libsox-fmt-mp3 ffmpeg sudo \
    git curl libssl-dev libreadline-dev zlib1g-dev

# Install Ruby 3.1 via rbenv, because our old Olaf version doesn't work with newer Ruby versions
ENV RBENV_ROOT="/usr/local/rbenv"
ENV PATH="$RBENV_ROOT/bin:$RBENV_ROOT/shims:$PATH"
RUN git clone https://github.com/rbenv/rbenv.git $RBENV_ROOT \
    && git clone https://github.com/rbenv/ruby-build.git $RBENV_ROOT/plugins/ruby-build \
    && rbenv install 3.1.7 \
    && rbenv global 3.1.7

RUN pip install yt-dlp pydub requests lxml python-dateutil eyeD3

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
