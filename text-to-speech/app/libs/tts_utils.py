from os import environ
from uuid import uuid1

from google.cloud import texttospeech

from .s3_utils import write_audio_to_s3fs
from config.config import SERVICE_NAME

environ["GOOGLE_APPLICATION_CREDENTIALS"] = "libs/subtle-signal-343106-40b3a61edcb2.json"


def text_to_speech(params):

    client = texttospeech.TextToSpeechClient()
    synthesis_input = texttospeech.SynthesisInput(ssml=params.text)

    voice = texttospeech.VoiceSelectionParams(
        language_code=params.language,
        name=params.voicename
    )

    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.LINEAR16,
        pitch=params.pitch,
        speaking_rate=params.speed
    )

    response = client.synthesize_speech(
        input=synthesis_input,
        voice=voice,
        audio_config=audio_config
    )

    file_key = str(uuid1())

    write_audio_to_s3fs(
        response.audio_content,
        bucket=SERVICE_NAME,
        key=f"{file_key}.mp3"
    )

    return file_key
