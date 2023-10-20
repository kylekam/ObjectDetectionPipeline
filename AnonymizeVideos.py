import ffmpeg
import os
import subprocess
import VideoUtils
import uuid
import json
from tqdm import tqdm
import csv

# VIDEO_PATH = "C:\\Users\\kkam\\Desktop\\tip_tracking_videos\\BRB.mp4"
VIDEO_DIR = "F:/AesculapVideos/Surgical Videos from Aeos/"
OUTPUT_DIR = "F:/AnonymizedVideos/"
OUTPUT_BEFORE_LOGS = "./output_logs/video_metadata_before.json"
OUTPUT_AFTER_LOGS = "./output_logs/video_metadata_after.json"
ANONYMIZED_LOGS = "./output_logs/anonymized_names.csv"

def main():
    if os.path.exists(VIDEO_DIR):
        print("Video directory exists")
    if not os.path.exists(OUTPUT_BEFORE_LOGS):
        open(OUTPUT_BEFORE_LOGS, 'x').close()
    if not os.path.exists(OUTPUT_AFTER_LOGS):
        open(OUTPUT_AFTER_LOGS, 'x').close()
    if not os.path.exists(ANONYMIZED_LOGS):
        open(ANONYMIZED_LOGS, 'x').close()

    # Get list of videos in VIDEO_DIR
    video_l = VideoUtils.getListOfVideos(VIDEO_DIR)

    outputMetadata(video_l, OUTPUT_BEFORE_LOGS)

    new_anon_video_names = []
    # Anonymize videos and output into OUTPUT_DIR
    for video_name in tqdm(video_l, desc="Anonymizing videos", colour="green"):
        # Generate name of new videos as a UUID
        new_anon_video_names.append(generateRandomUUID() + video_name[video_name.rfind("."):])

        curr_video_anonymized = os.path.join(OUTPUT_DIR,new_anon_video_names[-1])
        command = [
            'ffmpeg',
            '-i', video_name,
            '-map_metadata', '-1',
            '-c:v', 'copy',
            '-c:a', 'copy',
            curr_video_anonymized
        ]
        subprocess.run(command)

    # Write the new anonymized video names to a csv file
    with open(ANONYMIZED_LOGS, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(zip(video_l, new_anon_video_names))

    new_anon_video_paths = [os.path.join(OUTPUT_DIR, video) for video in new_anon_video_names]

    # Output metadata of anonymized videos
    outputMetadata(new_anon_video_paths, OUTPUT_AFTER_LOGS)

def generateRandomUUID():
    """
    Generates random UUID in the following format
    8-4-4-12 (num of characters)
    For example: 14e21bfb-0574-4981-89fb-8ee50312b999
    @return: random UUID
    """
    return str(uuid.uuid4())
    
def outputMetadata(_video_list, _output_path):
    dict_l = []
    for video in tqdm(_video_list, desc="Outputting video metadata", colour="green"):
        vid = ffmpeg.probe(video)
        dict_l.append(vid['streams'])
    with open(_output_path, 'w') as fp:
        json.dump(dict_l, fp, indent=4)

if __name__ == "__main__":
    main()