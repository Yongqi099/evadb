import os
import shutil
from typing import Dict, List

import pandas as pd

import evadb

from pytube import YouTube, extract
from youtube_transcript_api import YouTubeTranscriptApi


MAX_CHUNK_SIZE = 1000
DEFAULT_VIDEO_LINK = "https://www.youtube.com/watch?v=0E_wXecn4DU&pp=ygUKZGFpbHkgZG9zZQ%3D%3D"

APP_SOURCE_DIR = os.path.abspath(os.path.dirname(__file__))
YT_CONST = "https://www.youtube.com/watch?v="

# temporary file paths
TRANSCRIPT_PATH = os.path.join("evadb_data", "tmp", "transcript.csv")
SUMMARY_PATH = os.path.join("evadb_data", "tmp", "summary.csv")

global video_links

def receive_user_input():
    """Receives user input.

    Returns:
        user_input (dict): global configurations
    """

    while True:
        # get Youtube video url
        video_link = str(input(
                "🌐 Enter the URL of the YouTube video (press Enter when done): "
            ))

        if video_link == "":
            break

        # Check if the URL is a valid YouTube URL
        if video_link.startswith(YT_CONST):
            youtube_id = video_link.split("v=")[1]
            youtube = YouTube(video_link)
            video_title = youtube.title
            video_links[youtube_id] = video_title 

        else:
            print("⚠️ Please enter a valid YouTube URL.\n")

def partition_transcript(raw_transcript: str):
    """Group video transcript elements when they are too large.

    Args:
        transcript (str): downloaded video transcript as a raw string.

    Returns:
        List: a list of partitioned transcript
    """
    if len(raw_transcript) <= MAX_CHUNK_SIZE:
        return [{"text": raw_transcript}]

    k = 2
    while True:
        if (len(raw_transcript) / k) <= MAX_CHUNK_SIZE:
            break
        else:
            k += 1
    chunk_size = int(len(raw_transcript) / k)

    partitioned_transcript = [
        {"text": raw_transcript[i : i + chunk_size]}
        for i in range(0, len(raw_transcript), chunk_size)
    ]
    if len(partitioned_transcript[-1]["text"]) < 30:
        partitioned_transcript.pop()
    return partitioned_transcript


def partition_summary(prev_summary: str):
    """Summarize a summary if a summary is too large.

    Args:
        prev_summary (str): previous summary that is too large.

    Returns:
        List: a list of partitioned summary
    """
    k = 2
    while True:
        if (len(prev_summary) / k) <= MAX_CHUNK_SIZE:
            break
        else:
            k += 1
    chunk_size = int(len(prev_summary) / k)

    new_summary = [
        {"summary": prev_summary[i : i + chunk_size]}
        for i in range(0, len(prev_summary), chunk_size)
    ]
    if len(new_summary[-1]["summary"]) < 30:
        new_summary.pop()
    return new_summary


def group_transcript(transcript: dict):
    """Group video transcript elements when they are too short.

    Args:
        transcript (dict): downloaded video transcript as a dictionary.

    Returns:
        str: full transcript as a single string.
    """
    new_line = ""
    for line in transcript:
        new_line += " " + line["text"]

    return new_line


def download_youtube_video_transcript(video_link: str):
    """Downloads a YouTube video's transcript.

    Args:
        video_link (str): url of the target YouTube video.
    """
    video_id = extract.video_id(video_link)
    print("⏳ Transcript download in progress...")
    transcript = YouTubeTranscriptApi.get_transcript(video_id)
    print("✅ Video transcript downloaded successfully.")
    return transcript


def generate_summary(cursor: evadb.EvaDBCursor):
    """Generate summary of a video transcript if it is too long (exceeds llm token limits)

    Args:
        cursor (EVADBCursor): evadb api cursor.
    """
    transcript_list = cursor.table("Transcript").select("text").df()["transcript.text"]
    if len(transcript_list) == 1:
        summary = transcript_list[0]
        df = pd.DataFrame([{"summary": summary}])
        df.to_csv(SUMMARY_PATH)

        cursor.drop_table("Summary", if_exists=True).execute()
        cursor.query(
            """CREATE TABLE IF NOT EXISTS Summary (summary TEXT(100));"""
        ).execute()
        cursor.load(SUMMARY_PATH, "Summary", "csv").execute()
        return

    generate_summary_rel = cursor.table("Transcript").select(
        "ChatGPT('summarize the video in detail', text)"
    )
    responses = generate_summary_rel.df()["chatgpt.response"]

    summary = ""
    for r in responses:
        summary += f"{r} \n"
    df = pd.DataFrame([{"summary": summary}])
    df.to_csv(SUMMARY_PATH)

    need_to_summarize = len(summary) > MAX_CHUNK_SIZE
    while need_to_summarize:
        partitioned_summary = partition_summary(summary)

        df = pd.DataFrame([{"summary": partitioned_summary}])
        df.to_csv(SUMMARY_PATH)

        cursor.drop_table("Summary", if_exists=True).execute()
        cursor.query(
            """CREATE TABLE IF NOT EXISTS Summary (summary TEXT(100));"""
        ).execute()
        cursor.load(SUMMARY_PATH, "Summary", "csv").execute()

        generate_summary_rel = cursor.table("Summary").select(
            "ChatGPT('summarize in detail', summary)"
        )
        responses = generate_summary_rel.df()["chatgpt.response"]
        summary = " ".join(responses)

        # no further summarization is needed if the summary is short enough
        if len(summary) <= MAX_CHUNK_SIZE:
            need_to_summarize = False

    # load final summary to table
    cursor.drop_table("Summary", if_exists=True).execute()
    cursor.query(
        """CREATE TABLE IF NOT EXISTS Summary (summary TEXT(100));"""
    ).execute()
    cursor.load(SUMMARY_PATH, "Summary", "csv").execute()


def generate_response(cursor: evadb.EvaDBCursor, question: str) -> str:
    """Generates question response with llm.

    Args:
        cursor (EVADBCursor): evadb api cursor.
        question (str): question to ask to llm.

    Returns
        str: response from llm.
    """

    if len(cursor.table("Transcript").select("text").df()["transcript.text"]) == 1:
        return (
            cursor.table("Transcript")
            .select(f"ChatGPT('{question}', text)")
            .df()["chatgpt.response"][0]
        )
    else:
        # generate summary of the video if its too long
        if not os.path.exists(SUMMARY_PATH):
            generate_summary(cursor)

        return (
            cursor.table("Summary")
            .select(f"ChatGPT('{question}', summary)")
            .df()["chatgpt.response"][0]
        )

def cleanup():
    """Removes any temporary file / directory created by EvaDB."""
    if os.path.exists("evadb_data"):
        shutil.rmtree("evadb_data")

def write_transcript_to_file(youtube_id, transcript):
    """Writes a transcript to a text file.

    Args:
        youtube_id (str): The YouTube video ID.
        transcript (str): The transcript to write.
    """
    with open(f"{youtube_id}.txt", "w") as file:
        file.write(transcript)

def list_transcripts():
    """Lists all available transcripts.

    Returns:
        transcripts (list): A list of available transcripts.
    """
    transcripts = [f for f in os.listdir() if f.endswith(".txt")]
    return transcripts

def read_transcript(youtube_id):
    """Reads a transcript from a text file.

    Args:
        youtube_id (str): The YouTube video ID.

    Returns:
        transcript (str): The transcript.
    """
    with open(f"{youtube_id}.txt", "r") as file:
        return file.read()


if __name__ == "__main__":

    print(
    "🔮 Welcome to EvaDB! This app lets you ask questions on any YouTube video.\nYou will only need to supply a Youtube URL.\n")

    try:
        # establish evadb api cursor
        cursor = evadb.connect().cursor()


        inputting = True
        while inputting:
            receive_user_input()
            inputting = str(input(
                "\nWould you like to add an additional Video? (enter 'yes' if so): ")).lower() in ["y", "yes"]

        for youtube_id, video_link in video_links["video_links"].items():
            transcript = download_youtube_video_transcript(video_link)
            write_transcript_to_file(youtube_id=youtube_id, transcript=transcript)


        raw_transcript_string = None

        # Group the list of transcripts into a single raw transcript.
        if transcript is not None:
            raw_transcript_string = group_transcript(transcript)

        # Partition the transcripts if they are too big to circumvent LLM token restrictions.
        if raw_transcript_string is not None:
            partitioned_transcript = partition_transcript(raw_transcript_string)
            df = pd.DataFrame(partitioned_transcript)
            df.to_csv(TRANSCRIPT_PATH)

        # load chunked transcript into table
        cursor.drop_table("Transcript", if_exists=True).execute()
        cursor.query(
            """CREATE TABLE IF NOT EXISTS Transcript (text TEXT(50));"""
        ).execute()
        cursor.load(TRANSCRIPT_PATH, "Transcript", "csv").execute()

        # get OpenAI key if needed
        try:
            api_key = os.environ["OPENAI_KEY"]
        except KeyError:
            api_key = str(input("🔑 Enter your OpenAI key: "))
            os.environ["OPENAI_KEY"] = api_key

        print("===========================================")
        print("🪄 Ask anything about the video!")
        ready = True
        while ready:
            question = str(input("Question (enter 'exit' to exit): "))
            if question.lower() == "exit": ready = False
            else:
                # Generate response with chatgpt udf
                print("⏳ Generating response (may take a while)...")
                response = generate_response(cursor, question)
                print("+--------------------------------------------------+")
                print("✅ Answer:")
                print(response)
                print("+--------------------------------------------------+")

        cleanup()
        print("✅ Session ended.")
        print("===========================================")
    except Exception as e:
        cleanup()
        print("❗️ Session ended with an error.")
        print(e)