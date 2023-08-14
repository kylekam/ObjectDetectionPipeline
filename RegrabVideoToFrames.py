from ctypes import sizeof
from os import listdir
from os.path import isfile, join, basename, splitext
import glob
import cv2
import VideoUtils
import json

EXISTING_IMAGE_DIR = "F:\\kyle_files\\image_labeling_yolov4\\results_verified\\"
EXISTING_VIDEO_DIR = "\\\\192.168.11.50\\TDS_Videos\\DeepLearning\\videoAnonymized\\TDS_LabelingSource_Used\\"
OUTPUT_DIR = "F:\\kyle_files\\image_labeling_yolov4\\python_script_test_DELETETHIS\\"

def main():
    # get list of existing images
    existing_image_list = VideoUtils.getListOfImages(EXISTING_IMAGE_DIR)
    video_to_frames = VideoUtils.getDictVideosToFrames(existing_image_list)
    video_keys = list(video_to_frames.keys())
    print("Numer of video files: ", len(video_keys))

    # go into list of videos and get images
    existing_video_list = VideoUtils.getListOfVideos(EXISTING_VIDEO_DIR)
    failed_frames = {}
    failed_frames_count = 0
    # for each video, copy all frames we need
    for video_path in existing_video_list:
        # get video name
        video_name = basename(video_path)
        video_name = video_name.split(".")[0] # removes extension
        
        if video_name in video_to_frames:
            # get relevant frames, and add to list of failed frames
            failed_frames[video_name] = VideoUtils.getSelectFramesFromVideo(video_path, video_to_frames[video_name], OUTPUT_DIR)
            failed_frames_count += len(failed_frames[video_name])
    
    # print json of frames that failed to copy
    print(f"# ----------------- Failed on {failed_frames_count} frames ----------------- #")
    with open("failed_frames.json", "w") as f:
        print(json.dump(failed_frames, f, indent=4))

if __name__ == "__main__":
    main()