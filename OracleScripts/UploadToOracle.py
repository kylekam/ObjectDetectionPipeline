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

# IMAGE_DIR = "F:\\kyle_files\\Tip_Tracking_Stuff"
# IMAGE_DIR = "C:\\Users\\kkam\\Desktop\\tip_tracking_dataset_2"
# IMAGE_DIR = "C:\\Users\\kkam\\Desktop\\temp_output"
# IMAGE_DIR = "C:\\Users\\kkam\\Desktop\\tip_tracking_dataset_5_images"
# IMAGE_DIR = "C:\\Users\\kkam\\Desktop\\tip_tracking_dataset_2_split\\batch_1.000"
IMAGE_DIR = "F:\\kyle_files\\image_labeling_yolov4\\all_images_filtered\\final"

BATCH_SIZE = 500
MISSED_FILES_JSON = "missed_files.json"
VERSION_NUMBER = 1

# Number of max processes allowed at a time
CONCURRENCY = 20

LABELS = [
    "Forceps",
    "Dissector",
    "Micro_Needle_Holder",
    "Suction_Cannula",
    "Bone_Drill",
    "Ultrasonic_Aspirator",
]

"""
Make sure that your oci is setup: https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm
Run 'oci setup config' after following the instructions
"""
def main():
    sema = mp.BoundedSemaphore(CONCURRENCY)
    checkNumThreadsAndCPUs()

    # OCI Setup
    config = oci.config.from_file("~/.oci/config", "DEFAULT")

    # config['region'] = 'us-sanjose-1'
    # compartment_id = "ocid1.compartment.oc1..aaaaaaaabgnhnke36wn27m7ar5sua34hbbzugxvniyymjvl4iuhkrzsemidq" # testing
    # compartment_id = "ocid1.compartment.oc1..aaaaaaaayxoy46cizaayaxd4vinvtkdavemhrsmfvgym7efkteue35tjsaca" # actual dataset
    # compartment_id = "ocid1.compartment.oc1..aaaaaaaayxoy46cizaayaxd4vinvtkdavemhrsmfvgym7efkteue35tjsaca" # tiptracking_labeling
    compartment_id = "ocid1.compartment.oc1..aaaaaaaajcq5drsooqi2hb4v74tbxkba27hqz5guhqfjiem3i6elifrjfn2q" # tiptracking_1
    # compartment_id = "ocid1.compartment.oc1..aaaaaaaa3aj25opwthzbq443gwzw26ywtodryq7z7rcc6dvh4wbjjp7md3sa" # tiptracking_2
    
    namespace_name = "idrvtcm33fob"

    # Create an Object Storage client
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    # Get list of all files to upload
    file_search_string = os.path.join(IMAGE_DIR, "**/*.jpeg")
    all_files = []
    for filename in glob.iglob(file_search_string, recursive=True):
        # print(filename)
        all_files.append(filename)
    num_files = len(all_files)

    existing_buckets = OracleUtils.getAllBucketNames(object_storage_client, namespace_name, compartment_id)

    # if buckets with "batch_" exist, fill existing bucket
    last_bucket_num = 0
    backfill_amount = 0
    if existing_buckets:
        existing_buckets.sort()
        last_bucket_name = existing_buckets[-1]
        last_bucket_num =int(last_bucket_name.split(".")[1])

        # Check if most recent bucket is full
        num_objects = OracleUtils.getNumOfObjectsInBucket(object_storage_client,namespace_name,last_bucket_name)
        # if not full, then fill the difference with images
        if num_objects < BATCH_SIZE:
            backfill_amount = BATCH_SIZE-num_objects

            start_idx = 0
            end_idx = backfill_amount if backfill_amount < num_files else num_files # ensures that we don't go out of range
            filebatch = all_files[start_idx:end_idx]
            OracleUtils.parallelUpload(filebatch, last_bucket_name, namespace_name, config, sema)
            all_files = all_files[end_idx:]
            
    # Create proper amount of buckets for num of images left
    num_buckets = int(math.ceil(float(num_files - backfill_amount)/BATCH_SIZE))
    buckets = []
    # fill buckets with images
    # create data labeling service
    
    # Check that bucket does not already exist
    ## get the last batch number and shift all numbers by that value
    for bucket_num in range(last_bucket_num, num_buckets+last_bucket_num):
        bucket_num += 1
        bucket_name = "batch_" + f"{VERSION_NUMBER}.{bucket_num:03d}"
        buckets.append(bucket_name)

        if bucket_name not in existing_buckets:
            # Create the bucket
            object_storage_client.create_bucket(namespace_name=namespace_name,
                                                create_bucket_details=oci.object_storage.models.CreateBucketDetails(
                                                    name=bucket_name,
                                                    compartment_id=compartment_id,
                                                )
            )
            print(f"Bucket '{bucket_name}' created successfully in compartment '{compartment_id}'")
        else:
            print(f"Bucket '{bucket_name}' already exists in compartment")

    # for bucket in tqdm(existing_buckets, desc="Deleting buckets:", colour="green"):
    #     deleteAllObjectsInBucket(object_storage_client, namespace_name, bucket)
    #     object_storage_client.delete_bucket(namespace_name,bucket)

    # deleteDatasets(config, compartment_id)

    # exit()

    # Upload images
    for i in range(num_buckets):
        start_idx = BATCH_SIZE*i
        end_idx = BATCH_SIZE*(i+1) if BATCH_SIZE*(i+1) < num_files else num_files # ensures that we don't go out of range
        filebatch = all_files[start_idx:end_idx]
        OracleUtils.parallelUpload(filebatch, buckets[i], namespace_name, config, sema)

    # DEPRECATED
    # Find which images are missing
    missed = {}
    for i in range(num_buckets):
        start_idx = BATCH_SIZE*i
        end_idx = BATCH_SIZE*(i+1) if BATCH_SIZE*(i+1) < num_files else num_files-1 # ensures that we don't go out of range
        filebatch = all_files[start_idx:end_idx]

        # Verify that all files were uploaded
        objects = OracleUtils.getObjectNamesInBucket(object_storage_client, namespace_name, buckets[i])
        missed[buckets[i]] = []
        for file_name in filebatch:
            if os.path.basename(file_name) not in objects:
                missed[buckets[i]].append(file_name)
        print(f"Files missed: '{len(missed[buckets[i]])}'")
    # Write missed files to json
    with open(MISSED_FILES_JSON, "w") as outfile:
        outfile.write(json.dumps(missed, indent=4))

    # Create data labeling service 
    for i in range(num_buckets):   
        response = OracleUtils.createDatasetFromBucket(object_storage_client, config, compartment_id, namespace_name, buckets[i], LABELS, BATCH_SIZE)
        print(response)

    # 5. Bonus: Create excel sheet

def checkNumThreadsAndCPUs():
    total_threads = psutil.cpu_count()/psutil.cpu_count(logical=False)
    print('You can run {} processes per CPU core simultaneously'.format(total_threads))
    total_cpu = mp.cpu_count()
    print(f"You have {total_cpu} CPU accessible")

if __name__ == "__main__":
    main()