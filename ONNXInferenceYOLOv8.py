import cv2
import numpy as np
import onnxruntime as ort
import VideoUtils
from PIL import Image
import os
from yolov8 import YOLOv8
from yolov8.utils import draw_detections 

# VIDEO_PATH = "F:\\kyle_files\\repos\\RefineImageDataset\\used_videos\\4828b9ac-f90f-4dc7-bab0-5264dcdbf1f5.avi"
VIDEO_PATH = "D:\Datasets\Samples\SurgicalVideos"
MODEL_PATH = ".\\models\\tool_tip_v4.onnx"
# MODEL_PATH = ".\\models\\yolov8s.onnx"
OUTPUT_PATH = "D:\TipTrackingStuff\TestOutputs"
VIDEO_FILE = "BRB.mp4"

CONF_THRESHOLD = 0.2
IOU_THRESHOLD = 0.5
TRACKED_CLASS = 0
MODEL_INPUT_WIDTH = 640
MODEL_INPUT_HEIGHT = 640
CLASS_ID = 0

def main():
    yolov8_detector = YOLOv8(MODEL_PATH, conf_thres=CONF_THRESHOLD, iou_thres=IOU_THRESHOLD)

    cv2.namedWindow("Detected Objects", cv2.WINDOW_NORMAL)

    # Open the video file
    video_full_path = os.path.join(VIDEO_PATH,VIDEO_FILE)
    cap = cv2.VideoCapture(video_full_path)
    print("Opened video ", video_full_path)

    # Read video properties
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    isStereo = VideoUtils.isDimsStereo(frame_width, frame_height)
    if isStereo:
        frame_width /= 2 # monoscopic

    # Create video writer
    output_file_name = os.path.join(OUTPUT_PATH,VIDEO_FILE.split(".")[0]+"_PROCESSED.mp4")
    fps = cap.get(cv2.CAP_PROP_FPS)
    output_video = cv2.VideoWriter(output_file_name, 0x7634706d, fps, (int(frame_width), frame_height))

    # Check if the video file opened successfully
    if not cap.isOpened():
        print("Error opening video file")
        return
    
    while cap.isOpened():
        # Press key q to stop
        if cv2.waitKey(1) == ord('q'):
            break

        try:
            # Read frame from the video
            ret, frame = cap.read()
            if not ret:
                break
        except Exception as e:
            print(e)
            continue

        # Update object localizer
        boxes, scores, class_ids = yolov8_detector(frame)
        tracked_boxes = []
        tracked_scores = []
        tracked_class_ids = []
        for i in range(boxes.shape[0]):
            if class_ids[i] == TRACKED_CLASS:
                tracked_boxes.append(boxes[i])
                tracked_scores.append(scores[i])
                tracked_class_ids.append(class_ids[i])

        combined_img = draw_detections(
            image=frame,
            boxes=tracked_boxes,
            scores=tracked_scores,
            class_ids=tracked_class_ids,
            mask_alpha=0.3
        )
        # cv2.imshow("Detected Objects", combined_img)
        # Write the processed frame to the output video file
        output_video.write(combined_img)

if __name__ == "__main__":
    main()