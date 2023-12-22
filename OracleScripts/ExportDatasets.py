import os
import oci
import glob
import multiprocessing as mp
import math
from tqdm import tqdm
import json
import psutil
import time
import mimetypes
import OracleUtils
import logging

from oci.data_labeling_service_dataplane.data_labeling_client import DataLabelingClient
from oci.data_labeling_service.data_labeling_management_client import DataLabelingManagementClient
from oci.data_labeling_service.models import ObjectStorageSourceDetails
from oci.data_labeling_service.models import DatasetFormatDetails
from oci.data_labeling_service.models import LabelSet
from oci.data_labeling_service.models import Label
from oci.data_labeling_service.models import CreateDatasetDetails
from oci.data_labeling_service.models import GenerateDatasetRecordsDetails
from oci.data_labeling_service_dataplane.models import CreateObjectStorageSourceDetails
from oci.data_labeling_service_dataplane.models import CreateRecordDetails

IMAGE_DIR = "F:\\kyle_files\\image_labeling_yolov4\\all_images_filtered\\final"
EXPORT_BUCKET = "export_records"
DEBUGGING = False
def main():
    # OCI Setup
    config = oci.config.from_file("~/.oci/config", "DEFAULT")
    if DEBUGGING:
        config['log_requests'] = True
        logging.basicConfig()

    # config['region'] = 'us-sanjose-1'
    src_compartment_id = "ocid1.compartment.oc1..aaaaaaaajcq5drsooqi2hb4v74tbxkba27hqz5guhqfjiem3i6elifrjfn2q" # tiptracking_1
    dst_compartment_id = "ocid1.compartment.oc1..aaaaaaaatgbucxeawbmd7zvvloev7aake7rdml7huew4pglvg4c5ahxq6aca" # datasets
    namespace_name = "idrvtcm33fob"

    bh = OracleUtils.BucketHelpers(config)
    dh = OracleUtils.DatasetHelpers(config, src_compartment_id)

    # Check that destination bucket exists
    response = bh.doesBucketExist(namespace_name, EXPORT_BUCKET)
    if response == False:
        bh.createBucket(namespace_name, dst_compartment_id, EXPORT_BUCKET)
    if bh.doesBucketExist(namespace_name, EXPORT_BUCKET) == False:
        print("Bucket still doesn't exist")
        return

    # # Export all datasets to EXPORT_BUCKET
    # existing_buckets = bh.getAllBucketNames(namespace_name, src_compartment_id)
    # for bucket in existing_buckets:
    #     dh.exportDatasetToBucket_Kyle(bh, EXPORT_BUCKET, src_compartment_id, bucket, namespace_name)

    # Create dict that contains all image names that are labeled
    exported_names = bh.getObjectNamesInBucket(namespace_name, EXPORT_BUCKET)
    src_bucket_names = bh.getAllBucketNames(namespace_name, src_compartment_id)
    labeled_names_dict = {}
    parent_export_paths = {}
    for name in exported_names:
        if not name.endswith(".jpeg"):
            continue
        batch_name = "_".join(name.split("/")[0].split("_",2)[:2])
        item_name = name.split("/")[-1]
        if batch_name not in labeled_names_dict:
            labeled_names_dict[batch_name] = [item_name]
        else:
            labeled_names_dict[batch_name].append(item_name)
        if batch_name not in parent_export_paths:
            parent_path = "/".join(name.split("/")[:-1])
            parent_export_paths[batch_name] = parent_path + "/"
        

    # Create dict that contains all images that are not labeled
    not_labeled_dict = {}
    for batch in src_bucket_names:
        object_names = bh.getObjectNamesInBucket(namespace_name, batch)
        for img_name in object_names:
            if "datalabelingdataset" in img_name:
                continue
            if img_name.endswith(".jpeg") and img_name not in labeled_names_dict[batch]:
                if batch not in not_labeled_dict:
                    not_labeled_dict[batch] = [img_name]
                else:
                    not_labeled_dict[batch].append(img_name)
            
    
    # Upload empty .txt file for each missing image and copy image to EXPORT_BUCKET
    for batch in not_labeled_dict:
        for img_name in tqdm(not_labeled_dict[batch]):
            # Copy image
            dst_img_name = parent_export_paths[batch] + img_name
            bh.copyObject(namespace_name, batch, EXPORT_BUCKET, img_name, dst_img_name)

            # Upload empty .txt file
            txt_name = img_name.split(".")[0] + ".txt"
            dst_txt_name = "/".join(parent_export_paths[batch].split("/")[:2]) + "/labels/" + txt_name
            empty_file = open(txt_name, "w")
            empty_file.close()
            bh.singleUpload(txt_name, EXPORT_BUCKET, dst_txt_name, namespace_name)
            os.remove(txt_name)
        print(f"Finished filling in missing images for {batch}.")

    # Move all data into an images folder and a labels folder
    all_objects = bh.getObjectsInBucket(namespace_name, EXPORT_BUCKET)
    for obj in tqdm(all_objects, "Moving into images and labels folders", colour="green"):
        # Rename object by copying with new name
        if obj.name.endswith(".jpeg"):
            new_name = "images/" + obj.name.split("/")[-1]
        elif obj.name.endswith(".txt"):
            new_name = "labels/" + obj.name.split("/")[-1]
        else:
            bh.deleteObject(namespace_name, EXPORT_BUCKET, obj.name)
            continue

        bh.renameObject(namespace_name, EXPORT_BUCKET, _new_name=new_name, _old_name=obj.name)

    # TODO: create training and testing split
        

if __name__ == "__main__":
    main()
