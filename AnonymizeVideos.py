import ffmpeg
import os
import subprocess

VIDEO_PATH = "C:\\Users\\kkam\\Desktop\\tip_tracking_videos\\BRB.mp4"

def main():
    if os.path.exists(VIDEO_PATH):
        print("Video exists")
    vid = ffmpeg.probe(VIDEO_PATH)
    print(vid['streams'])

    command = [
        'ffmpeg',
        '-i', '*.mp4',
        '-map_metadata', '-1',
        '-c:v', 'copy',
        '-c:a', 'copy',
        'out.mp4'
    ]
    subprocess.run(command)


if __name__ == "__main__":
    main()