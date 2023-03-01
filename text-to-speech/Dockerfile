FROM python:3.8-slim-buster

WORKDIR /source

COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app/ .

ENTRYPOINT ["python", "-u", "./app.py"]
