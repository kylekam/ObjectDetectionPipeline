import cv2
import os

folder_src = "F:\\kyle_files\\image_labeling_yolov8\\demo_room_images"
folder_dst = "F:\\kyle_files\\image_labeling_yolov8\\demo_room_images_left"

for image_path in os.listdir(folder_src):
    if image_path.endswith('.jpg'):
        full_path = os.path.join(folder_src, image_path)
        full_path_dst = os.path.join(folder_dst, image_path)
        img = cv2.imread(full_path,cv2.IMREAD_COLOR)
        left_half = img[0:int(img.shape[0]), 0:int(img.shape[1]/2)]
        cv2.imwrite(full_path_dst, left_half)

cv2.destroyAllWindows()