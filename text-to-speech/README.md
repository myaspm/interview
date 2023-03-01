A simple project which uses Google Cloud Text To Speech API to generate speech sound files to be used in custom videos.

Parameters are read from a Redis Streams topic, output mp3 file is written to a S3 bucket and file key is returned to another Redis Streams topic to be read by another application.
