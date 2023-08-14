import os
import pandas as pd

test_img_path = "F:\\\kyle_files\\image_labeling_yolov8\\yolov8\\v3\\test"
val_img_path = "F:\\\kyle_files\\image_labeling_yolov8\\yolov8\\v3\\val"
train_img_path = "F:\\\kyle_files\\image_labeling_yolov8\\yolov8\\v3\\train"

test_image_list = [x for x in os.listdir(test_img_path) if x.endswith('.jpg')]
val_image_list = [x for x in os.listdir(val_img_path) if x.endswith('.jpg')]
train_image_list = [x for x in os.listdir(train_img_path) if x.endswith('.jpg')]

test_label_list = [x for x in os.listdir(test_img_path) if x.endswith('.txt')]
val_label_list = [x for x in os.listdir(val_img_path) if x.endswith('.txt')]
train_label_list = [x for x in os.listdir(train_img_path) if x.endswith('.txt')]

image_list = test_image_list + val_image_list + train_image_list
label_list = test_label_list + val_label_list + train_label_list
print(len(image_list))
print(len(label_list))

df = pd.DataFrame()
df['images'] = image_list
df['labels'] = label_list

df.to_csv('tooltipdataset_v3.csv')