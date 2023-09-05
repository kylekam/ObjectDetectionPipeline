import albumentations as A
import cv2
import os
import numpy as np

IMAGE_DIR = "E:\\Videos\\deletethis_test"
IMAGE_NAME = "1cf27d42-1dba-4738-b09f-fa4ea341c936_FRM_000000_of_036004"
LABEL_DIR = "E:\\Videos\\deletethis_test"


# Augmentations
transform = A.Compose([
    A.HorizontalFlip(p=0.99),
], bbox_params=A.BboxParams(format='yolo', label_fields=['class_labels']))

# Read images and bounding boxes from the disk
image = cv2.imread(os.path.join(IMAGE_DIR,IMAGE_NAME + ".jpg"))
image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

annotatationFile = open(os.path.join(LABEL_DIR,IMAGE_NAME+".txt"), 'r')
annotations = annotatationFile.readlines()
annotatationFile.close()

bboxes = []
class_labels = []
for bbox in annotations:
    classId, x_cen, y_cen, w, h = map(float, bbox.split(" "))
    bboxes.append([x_cen, y_cen, w, h])
    class_labels.append([classId])

# Apply transformation to image and annotation
transformed = transform(image=image, bboxes=bboxes, class_labels=class_labels)
transformed_image = transformed['image']
transformed_bboxes = transformed['bboxes']
transformed_class_labels = transformed['class_labels']

# Display image
transformed_image = cv2.cvtColor(transformed_image, cv2.COLOR_RGB2BGR)
cv2.imshow("Window",transformed_image)
cv2.waitKey()


