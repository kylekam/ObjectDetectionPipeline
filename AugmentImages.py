import albumentations as A
import imgaug as ia
from imgaug.augmentables.bbs import BoundingBox, BoundingBoxesOnImage
from imgaug import augmenters as iaa 
import cv2
import os
import numpy as np
import matplotlib.pyplot as plt
import math
import argparse

# https://github.com/aleju/imgaug
IMAGE_DIR = "D:\\annotated_images\\tiptracking_samples"
IMAGE_NAME = "1cf27d42-1dba-4738-b09f-fa4ea341c936_FRM_004700_of_036004"
LABEL_DIR = "D:\\annotated_images\\tiptracking_samples"
NUM_AUG = 16
IMG_TILE_HEIGHT = 300

# IMAGE_DIR = "C:\\Users\\kkam\\repos\\Images\\annotated_image_examples"
# IMAGE_NAME = "1cf27d42-1dba-4738-b09f-fa4ea341c936_FRM_004700_of_036004"
# LABEL_DIR = "C:\\Users\\kkam\\repos\\Images\\annotated_image_examples"

def imgaug_main():
    ia.seed(1)
    
    # Read in image
    image = cv2.imread(os.path.join(IMAGE_DIR,IMAGE_NAME + ".jpg"))
    image = image_resize(image, height = IMG_TILE_HEIGHT)
    # image = ia.imresize_single_image(image, (298, 447))

    # Read in bounding boxes
    bboxes = []
    with open(os.path.join(LABEL_DIR, IMAGE_NAME + ".txt")) as file:
        annotations = file.readlines()
        for annotation in annotations:
            bbox = yoloToCv(image,annotation)
            bboxes.append(bbox)

    bboxes_aug = BoundingBoxesOnImage([
        BoundingBox(x1=bbox[0], x2=bbox[1], y1=bbox[2], y2=bbox[3]) for bbox in bboxes
    ], shape=image.shape)

    # Define augmentation sequence
    seq = iaa.Sequential([
        iaa.GammaContrast(1.5),
        iaa.Affine(translate_percent={"x": 0.1}, scale=0.8)
    ])
    seq_det = seq.to_deterministic()
    
    # Apply augmentations
    img_list_aug = seq_det.augment_images([image for i in range(NUM_AUG)])
    bboxes_list_aug = seq_det.augment_bounding_boxes([bboxes_aug for i in range(NUM_AUG)])

    # Draw boxes on image
    img_list_aug = [bbox.draw_on_image(img) for img, bbox in zip(img_list_aug, bboxes_list_aug)]

    img_grid = np.array(ia.draw_grid(img_list_aug))

    # Show the final grid
    cv2.namedWindow("BBox on Images", cv2.WINDOW_NORMAL)
    cv2.setWindowProperty("BBox on Images", cv2.WND_PROP_FULLSCREEN,cv2.WINDOW_FULLSCREEN)
    cv2.imshow("BBox on Images", img_grid)
    cv2.waitKey()

def albumentation_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hflip", type=float, help="horizontal flip probability", default=0.5)
    parser.add_argument("--vflip", type=float, help="vertical flip probability", default=0.5)
    parser.add_argument("--hsv", type=float, help="hue saturation value prob")
    args = parser.parse_args()

    # Augmentations
    # scale
    # hflip
    # vflip
    # hsv
    # resize <---
    # crop <---
    # color jitter
    # rotate
    # mosaic <-- tiles images together and then performs a random cutout
    # mixup
    transform = A.Compose([
        A.RandomScale(p=0.5),
        A.HorizontalFlip(p=args.hflip),
        A.VerticalFlip(p=args.vflip),
        # A.RandomCrop(width=450, height=450),
        A.RandomBrightnessContrast(p=0.2),
        # A.RandomSizedBBoxSafeCrop(height=500, width=500, p=1),
        A.HueSaturationValue(p=1),
        A.ColorJitter(p=1),
        A.Rotate(p=1)
    ], bbox_params=A.BboxParams(format='yolo', min_visibility=0.3, label_fields=['class_labels']))
    #TODO: control whether crop or bbox crop is being used

    # Read images and bounding boxes from the disk
    image_orig = cv2.imread(os.path.join(IMAGE_DIR,IMAGE_NAME + ".jpg"))
    image = cv2.cvtColor(image_orig, cv2.COLOR_BGR2RGB)

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
    img_list = [image for i in range(NUM_AUG)]
    bbox_list = [bboxes for i in range(NUM_AUG)]
    transformed_list = []
    transformed_img_list = []
    transformed_bbox_list = []
    transformed_class_labels = []

    for i in range(len(img_list)):
        transformed_list.append(transform(image=img_list[i], 
                                        bboxes=bbox_list[i], 
                                        class_labels=class_labels))
        transformed_img_list.append(transformed_list[i]['image'])
        transformed_bbox_list.append(transformed_list[i]['bboxes'])
        transformed_class_labels.append(transformed_list[i]['class_labels'])
        transformed_img_list[i] = cv2.cvtColor(transformed_img_list[i], cv2.COLOR_RGB2BGR)

    # Draw boxes on image
    gridShowBoundingBoxes(transformed_img_list, transformed_bbox_list)
    for i in range(len(transformed_img_list)):
        transformed_img_list[i] = showBoundingBox(transformed_img_list[i], transformed_bbox_list[i])

    img_final = create_img_gallery(np.array(transformed_img_list))

    
    # Show the final grid
    ratio = image_orig.shape[0] / image_orig.shape[1] 
    image_orig = cv2.resize(image_orig, (500, int(500*ratio)),
                            interpolation=cv2.INTER_AREA)

    cv2.imshow("Original", image_orig)

    cv2.namedWindow("BBox on Images", cv2.WINDOW_NORMAL)
    cv2.setWindowProperty("BBox on Images", cv2.WND_PROP_FULLSCREEN,cv2.WINDOW_FULLSCREEN)
    cv2.imshow("BBox on Images", img_final)
    cv2.waitKey()

def yoloToCv(_img, _annotation):
    """
    Returns top left corner and bottom right corner.
    """
    dh, dw, _ = _img.shape
    _, x, y, w, h = map(float, _annotation.split(' '))

    x1 = int((x - w / 2) * dw)
    x2 = int((x + w / 2) * dw)
    y1 = int((y - h / 2) * dh)
    y2 = int((y + h / 2) * dh)

    return x1,x2,y1,y2

def image_resize(image, width = None, height = None, inter = cv2.INTER_AREA):
    # initialize the dimensions of the image to be resized and
    # grab the image size
    dim = None
    (h, w) = image.shape[:2]

    # if both the width and height are None, then return the
    # original image
    if width is None and height is None:
        return image

    # check to see if the width is None
    if width is None:
        # calculate the ratio of the height and construct the
        # dimensions
        r = height / float(h)
        dim = (int(w * r), height)

    # otherwise, the height is None
    else:
        # calculate the ratio of the width and construct the
        # dimensions
        r = width / float(w)
        dim = (width, int(h * r))

    # resize the image
    resized = cv2.resize(image, dim, interpolation = inter)

    # return the resized image
    return resized

def gridShowBoundingBoxes(_imgs, _annotations):
    screen_size = (1920,1080)
    num_elements = len(_imgs)
    tile_per_side = int(math.sqrt(num_elements))
    tile_size = (int(screen_size[0]/tile_per_side),
                 int((screen_size[1]/tile_per_side)))

    for i, _ in enumerate(_imgs):
        _imgs[i] = cv2.resize(_imgs[i], tile_size, interpolation=cv2.INTER_AREA) 
        showBoundingBox(_imgs[i], _annotations[i])

def showBoundingBox(_img, _annotations):
    dh, dw, _ = _img.shape

    for dt in _annotations:
        if (isinstance(dt, str)):
            # Split string to float
            _, x, y, w, h = map(float, dt.split(' '))
        else:
            x, y, w, h =  dt

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


def create_img_gallery(array, ncols=3):
    nindex, height, width, intensity = array.shape

    ncols = int(math.sqrt(nindex))

    nrows = nindex//ncols
    assert nindex == nrows*ncols
    # want result.shape = (height*nrows, width*ncols, intensity)
    result = (array.reshape(nrows, ncols, height, width, intensity)
              .swapaxes(1,2)
              .reshape(height*nrows, width*ncols, intensity))
    return result

def display_images_in_grid(image_paths, rows, cols):
    """
    Display images in a grid layout.

    Args:
        image_paths (list): List of image file paths.
        rows (int): Number of rows in the grid.
        cols (int): Number of columns in the grid.

    Raises:
        ValueError: If the number of images doesn't match the grid size.
    """
    num_images = len(image_paths)
    if num_images != rows * cols:
        raise ValueError("Number of images must match the grid size.")

    fig, axes = plt.subplots(rows, cols, figsize=(12, 8))

    for i, ax in enumerate(axes.ravel()):
        img = mpimg.imread(image_paths[i])
        ax.imshow(img)
        ax.axis('off')

    plt.subplots_adjust(wspace=0.1, hspace=0.1)
    plt.show()

if __name__ == "__main__":
    albumentation_main()