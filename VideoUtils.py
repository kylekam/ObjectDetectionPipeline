from ctypes import sizeof
from os import listdir
from os.path import isfile, join, basename, splitext
import glob
import shutil
import cv2

def getListOfVideos(_dir):
    """Returns list of videos found in _dir""" 
    types = ("*.avi", "*.mp4")
    video_list = []
    for files in types:
        video_list.extend(glob.glob(_dir + files))
    print("Numer of video files: ", len(video_list))
    return video_list

def isFrameStereo(_frame):
    """Checks if image is stereo"""
    return _frame.shape[1] > _frame.shape[0]*2

def isDimsStereo(_width, _height):
    """Check if image is stereo"""
    return _width > _height*2

def getLeftImage(_frame):
    """Use with isStereo. Returns the left half of the stereo image."""
    return _frame[0:_frame.shape[0],0:_frame.shape[1]//2]
        
def getRightImage(_frame):
    """Use with isStereo. Returns the right half of the stereo image."""
    return _frame[0:_frame.shape[0],_frame.shape[1]//2:]

def getFrameFromVideo(_videoName, _frameRate, _outputDir):
    """Gets a frame from _videoName every _frameRate frames. Dumps the
    images into _outputDir."""
    print("Trying to open ", _videoName)
    cap = cv2.VideoCapture(_videoName)
    if (cap.isOpened()):
        print("Opened ", _videoName, " SUCCESSFULLY!")
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        for frame_num in range(0, total_frames, _frameRate):
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
            ret, frame = cap.read()
            if ret == False:
                print("break on " + _videoName + " frame num: " + str(frame_num))
                break

            # if two images then grab left
            if isFrameStereo(frame):
                frame = getLeftImage(frame)

            # output images into external folder
            vidName = splitext(basename(_videoName))[0]
            cv2.imwrite(_outputDir + vidName + "_FRM_" + f'{frame_num:06}' + "_of_" + f'{int(cap.get(cv2.CAP_PROP_FRAME_COUNT)):06}' + ".jpg",frame)
    else:
        print("Failed to open ", _videoName)

def getList(dict):
    list = []
    for key in dict.keys():
        list.append(key)
         
    return list
    
def copyFiles(_src, _dst, _fileNames):
    for f in _fileNames:
        src = _src + f
        dst = _dst + f
        try:
            shutil.copyfile(src + ".avi", dst + ".avi")
        except FileNotFoundError:
            shutil.copyfile(src + ".mp4", dst + ".mp4")
        print("copied " + f)

def getListOfImages(_dir):
    """
    Returns list of images in _dir.

    @return list of images
    """
    types = ("*.jpg", "*.png")
    image_list = []
    for files in types:
        image_list.extend(glob.glob(_dir + "\\" + files))
    print("Numer of image files: ", len(image_list))
    return image_list

def getDictVideosToFrames(_list):
    """
        _list: list of images 
    """
    video_to_frames = {}
    for f in _list:
        parse = basename(f)
        parse = parse.split("_FRM_", 2)
        videoName = parse[0]
        frameNumber = parse[1].split("_of")[0]

        if videoName not in video_to_frames:
            video_to_frames.update({videoName: [int(frameNumber)]})
        else:
            video_to_frames[videoName].append(frameNumber)
    return video_to_frames

def getSelectFramesFromVideo(_videoPath, _frameList, _outputDir):
    """
    Gets the dict values corresponding to _videoToFrameDict[_videoName] and returns 
    a list of the frames.
        @param _videoPath: full video path
        @param _videoToFrameDict: dictionary from video name to frame number
        @param _outputDir: image dump directory

        @return list of frames that failed to copy for _videoPath
    """
    print("Trying to open ", _videoPath)
    cap = cv2.VideoCapture(_videoPath)
    videoName = basename(_videoPath)
    _failedFrameList = _frameList.copy()
    if (cap.isOpened()):
        print("Opened ", videoName, " SUCCESSFULLY!")
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        for f in _frameList:        
            frameNum = int(f)
            errorNum = frameNum
            cap.set(cv2.CAP_PROP_POS_FRAMES, frameNum)
            ret, frame = cap.read()
            if ret == False:
                print("break on " + videoName + " frame num: " + str(frameNum))
                break

            # if two images then grab left
            if isFrameStereo(frame):
                frame = getLeftImage(frame)

            # output images into external folder
            cv2.imwrite(_outputDir + splitext(videoName)[0] + "_FRM_" + f'{frameNum:06}' + "_of_" + f'{total_frames:06}' + ".jpg",frame)
            _failedFrameList.remove(f)
    else:
        print("Failed to open ", videoName)
    
    return _failedFrameList