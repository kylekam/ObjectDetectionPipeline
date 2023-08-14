from ctypes import sizeof
from os import listdir
from os.path import isfile, join, basename, splitext
import glob
import shutil
import cv2
import VideoUtils
VIDEO_DIR = "F:\\kyle_files\\image_labeling_yolov8\\AnimalLabJune2023"
IMAGE_DIR = "F:\\kyle_files\\image_labeling_yolov8\\AnimalLabJune2023\\raw_images\\"
FRAME_RATE = 20

def main():
    # get list of videos
    video_list = VideoUtils.getListOfVideos(VIDEO_DIR)

    # get frames from videos
    for video_path in video_list:
        video_name = basename(video_path)
        VideoUtils.getFrameFromVideo(video_name, FRAME_RATE, IMAGE_DIR)

if __name__ == "__main__":
    main()

exit()

### Read in video file names in F:\kyle_files\image_labeling\results_verified
images = glob.glob("F:\\kyle_files\\image_labeling_yolov4\\results_rejected\\*.jpg")

# print(len(images))
# print(images)

parsing = []
video_to_frames = {}
for f in images:
    parse = f.split("rejected\\",1)[1]
    parse = parse.split("_FRM_", 2)
    videoName = parse[0]
    frameNumber = parse[1].split("_of")[0]

    if videoName not in video_to_frames:
        video_to_frames.update({videoName: [int(frameNumber)]})
    else:
        video_to_frames[videoName].append(frameNumber)
    
print(getList(video_to_frames))
print("Numer of video files: ", len(getList(video_to_frames)))

### Move video files from server to local machine
src = "\\\\192.168.11.50\\TDS_Videos\\DeepLearning\\videoAnonymized\\TDS_LabelingSource_Used\\"
dst = "\\\\192.168.11.50\\TDS_Videos\\DeepLearning\\videoAnonymized\\TDS_LabelingSource_Used\\"
# copyFiles(src, dst, video_to_frames)
# print("copy completed")

### Get frames from videos
image_dir = "F:\\kyle_files\\image_labeling_yolov8\\image_dump\\"
successful_frames = {}
for v in video_to_frames:
    print("Trying to open ", v)
    cap = cv2.VideoCapture(dst + v + ".avi")
    if (cap.isOpened()):
        print("Opened ", v, ".avi SUCCESSFULLY!")
        for f in video_to_frames[v]:        
            frameNum = int(f)
            cap.set(cv2.CAP_PROP_POS_FRAMES, frameNum)
            ret, frame = cap.read()
            if ret == False:
                print("break on " + v + ".avi" + " frame num: " + str(f))
                break

            # if two images then grab left
            if frame.shape[1] > frame.shape[0]*2:
                frame = frame[0:frame.shape[0],0:frame.shape[1]//2]

            # output images into external folder
            cv2.imwrite(image_dir + v + "_FRM_" + f'{frameNum:06}' + "_of_" + f'{int(cap.get(cv2.CAP_PROP_FRAME_COUNT)):06}' + ".jpg",frame)
            # if successful add to dict 
            if v not in successful_frames:
                successful_frames.update({v: [frameNum]})
            else:
                successful_frames[v].append(frameNum)


    cap = cv2.VideoCapture(dst + v + ".mp4")
    if (cap.isOpened()): 
        print("Opened ", v, ".mp4 SUCCESSFULLY!")
        for f in video_to_frames[v]:
            frameNum = int(f)
            cap.set(cv2.CAP_PROP_POS_FRAMES, frameNum)
            ret, frame = cap.read()
            if ret == False:
                print("break on " + v + ".mp4" + " frame num: " + str(f))
                break

            # if two images then grab left
            if frame.shape[1] > frame.shape[0]*2:
                frame = frame[0:frame.shape[0],0:frame.shape[1]//2]

            # output images into external folder
            cv2.imwrite(image_dir + v + "_FRM_" + f'{frameNum:06}' + "_of_" + f'{int(cap.get(cv2.CAP_PROP_FRAME_COUNT)):06}' + ".jpg",frame)
            if v not in successful_frames:
                successful_frames.update({v: [frameNum]})
            else:
                successful_frames[v].append(frameNum)
    print("Finished with " + v)

# add all failed frames to dict
failed_frames = {}
for v in video_to_frames:
    for frameNum in video_to_frames[v]:
        # if video exists in successful_frames
        if v in successful_frames:
            # if frame isn't successful
            if frameNum not in successful_frames[v]:
                if v not in failed_frames:
                    failed_frames.update({v: [frameNum]})
                else:
                    failed_frames[v].append(frameNum)
        # if video doesn't exist in successful_frames
        else:
            if v not in failed_frames:
                failed_frames.update({v: [frameNum]})
            else:
                failed_frames[v].append(frameNum)



print("Failed videos")
print(failed_frames.items())
print("Failed frames dictionary")
print("Number of failed frames", sum(len(failed_frames[x]) for x in failed_frames))
print(failed_frames)

