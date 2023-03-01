# Video Tools

A custom video generator. 

* Can overlay images, texts and/or another video on top of a video. 
* Can concat videos.
* Can create HLS streams and M3U8 files from existing videos.
* Can create a n seconds length video from a video frame/color/picture.
* Can add/edit/delay etc. sound to a video.

Parameters are read from Redis Streams. Original videos must reside in an S3 bucket. Outputs will also be written to S3.
