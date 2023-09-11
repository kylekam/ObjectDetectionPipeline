import albumentations as A
import cv2
import os
import numpy as np

# IMAGE_DIR = "E:\\Videos\\deletethis_test"
# IMAGE_NAME = "1cf27d42-1dba-4738-b09f-fa4ea341c936_FRM_000000_of_036004"
# LABEL_DIR = "E:\\Videos\\deletethis_test"

IMAGE_DIR = "C:\\Users\\kkam\\repos\\Images\\annotated_image_examples"
IMAGE_NAME = "1cf27d42-1dba-4738-b09f-fa4ea341c936_FRM_004700_of_036004"
LABEL_DIR = "C:\\Users\\kkam\\repos\\Images\\annotated_image_examples"

def main():
    # Augmentations
    transform = A.Compose([
        A.HorizontalFlip(p=0.99),
    ], bbox_params=A.BboxParams(format='yolo', min_visibility=0.3, label_fields=['class_labels']))

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

    print(transformed_image.size)
    # concatenate image Horizontally
    img_size_1 = (416,416)
    img_size_2 = (640,640)
    image = cv2.cvtColor(image,cv2.COLOR_RGB2BGR)
    image = cv2.resize(image, img_size_1)
    transformed_image = cv2.resize(transformed_image, img_size_1)

    # add bounding boxes
    image = showBoundingBox(image, annotations)
    transformed_annotation = annotations
    transformed_image = showBoundingBox(transformed_image, transformed_bboxes)
    hori = np.concatenate((image, transformed_image), axis=1)

    cv2.imshow("Original vs Transformed",hori)

    cv2.waitKey()

def showBoundingBox(_img, _annotations):
    dh, dw, _ = _img.shape

    for dt in _annotations:

        # Split string to float
        _, x, y, w, h = map(float, dt.split(' '))

        # Taken from https://github.com/pjreddie/darknet/blob/810d7f797bdb2f021dbe65d2524c2ff6b8ab5c8b/src/image.c#L283-L291
        # via https://stackoverflow.com/questions/44544471/how-to-get-the-coordinates-of-the-bounding-box-in-yolo-object-detection#comment102178409_44592380
        l = int((x - w / 2) * dw)
        r = int((x + w / 2) * dw)
        t = int((y - h / 2) * dh)
        b = int((y + h / 2) * dh)
        
        if l < 0:
            l = 0
        if r > dw - 1:
            r = dw - 1
        if t < 0:
            t = 0
        if b > dh - 1:
            b = dh - 1

        cv2.rectangle(_img, (l, t), (r, b), (0, 0, 255), 1)

    return _img

if __name__ == "__main__":
    main()