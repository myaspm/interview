FROM python:3.10-slim-buster

WORKDIR /source

COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

RUN apt update -y && apt-get install ffmpeg imagemagick -y

RUN mv /etc/ImageMagick-6/policy.xml /etc/ImageMagick-6/policy.xmlout

COPY app/ .

COPY ffmpeg_reader.py /usr/local/lib/python3.8/site-packages/moviepy/video/io/

ENTRYPOINT ["python", "-u", "./app.py"]
