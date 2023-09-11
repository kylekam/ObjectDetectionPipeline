import os
import oci
import glob
import multiprocessing as mp
import math
from tqdm import tqdm
import json
import psutil
import time

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
IMAGE_DIR = "C:\\Users\\kkam\\repos\\Images\\delete_this_test_jpeg"
BATCH_SIZE = 1000
MISSED_FILES_JSON = "missed_files.json"

# Number of max processes allowed at a time
CONCURRENCY= 7


def main():
    sema = mp.BoundedSemaphore(CONCURRENCY)
    checkNumThreadsAndCPUs()

    # OCI Setup
    config = oci.config.from_file("~/.oci/config", "DEFAULT")
    compartment_id = "ocid1.compartment.oc1..aaaaaaaabgnhnke36wn27m7ar5sua34hbbzugxvniyymjvl4iuhkrzsemidq"
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

    # Create proper amount of buckets
    num_buckets = int(math.ceil(float(num_files)/BATCH_SIZE))
    buckets = []
    existing_buckets = getAllBucketNames(object_storage_client, namespace_name, compartment_id)

    # # Check that bucket does not already exist
    # for bucket_num in range(num_buckets):
    #     bucket_num += 1
    #     bucket_name = "batch_" + f"{bucket_num:03d}"
    #     buckets.append(bucket_name)

    #     if bucket_name not in existing_buckets:
    #         # Create the bucket
    #         object_storage_client.create_bucket(namespace_name=namespace_name,
    #                                             create_bucket_details=oci.object_storage.models.CreateBucketDetails(
    #                                                 name=bucket_name,
    #                                                 compartment_id=compartment_id,
    #                                             )
    #         )
    #         print(f"Bucket '{bucket_name}' created successfully in compartment '{compartment_id}'")
    #     else:
    #         print(f"Bucket '{bucket_name}' already exists in compartment")

    # # Uncomment this to delete objects from bucket
    # deleteAllObjectsInBucket(object_storage_client, namespace_name, buckets[4])
    # exit()

    # # Upload images
    # for i in range(3,num_buckets):
    #     start_idx = BATCH_SIZE*i
    #     end_idx = BATCH_SIZE*(i+1) if BATCH_SIZE*(i+1) < num_files else num_files-1 # ensures that we don't go out of range
    #     filebatch = all_files[start_idx:end_idx]
    #     parallelUpload(filebatch, buckets[i], namespace_name, config, sema)

    # # Find which images are missing
    # missed = {}
    # for i in range(num_buckets):
    #     start_idx = BATCH_SIZE*i
    #     end_idx = BATCH_SIZE*(i+1) if BATCH_SIZE*(i+1) < num_files else num_files-1 # ensures that we don't go out of range
    #     filebatch = all_files[start_idx:end_idx]

    #     # Verify that all files were uploaded
    #     objects = getObjectNamesInBucket(object_storage_client, namespace_name, buckets[i])
    #     missed[buckets[i]] = []
    #     for file_name in filebatch:
    #         if os.path.basename(file_name) not in objects:
    #             missed[buckets[i]].append(file_name)
    #     print(f"Files missed: '{len(missed[buckets[i]])}'")

    # # Write missed files to json
    # with open(MISSED_FILES_JSON, "w") as outfile:
    #     outfile.write(json.dumps(missed, indent=4))

    # Create data labeling service 
    temp_bucket_name = "delete_this_test_jpeg"
    response = createDatasetFromBucket(object_storage_client, config, compartment_id, namespace_name, temp_bucket_name)
    print(response)

    # 5. Bonus: Create excel sheet

def parallelUpload(_src_list: list, _dst_bucket: str, _namespace_name: str, _config, _sema):
    """
    parallelUpload will upload files concurrently to Oracle buckets.
    """

    # print("Starting upload for {}".format(_dst_bucket))
    proc_list = []
    for file_path in tqdm(_src_list, desc=f"Uploading {_dst_bucket}"):
        _sema.acquire()
        # print("Starting upload for {}".format(file_path))
        p = mp.Process(target=upload_to_object_storage, args=(_config,
                                                        _namespace_name,
                                                        _dst_bucket,
                                                        file_path,
                                                        _sema))
        p.start()
        proc_list.append(p)

    # Upload files
    for job in tqdm(proc_list, desc=f"Verifying {_dst_bucket}"):
        job.join()

def getAllBucketNames(_object_storage_client, _namespace_name, _compartment_id):
    # List the buckets in the compartment
    buckets = _object_storage_client.list_buckets(namespace_name=_namespace_name, compartment_id=_compartment_id)

    bucket_list = []
    for bucket in buckets.data:
        bucket_list.append(bucket.name)

    return bucket_list

def deleteAllObjectsInBucket(_object_storage_client, _namespace_name, _bucket_name):
    # List the objects in the bucket
    list_objects_response = _object_storage_client.list_objects(namespace_name=_namespace_name, bucket_name=_bucket_name)

    # Iterate through the objects and delete them
    for obj in tqdm(list_objects_response.data.objects, desc=f"Deleting {_bucket_name} objects"):
        object_name = obj.name

        # Delete the object
        _object_storage_client.delete_object(namespace_name=_namespace_name, bucket_name=_bucket_name, object_name=object_name)

def getNumOfObjectsInBucket(_object_storage_client, _namespace_name, _bucket_name):
    # List the objects in the bucket
    list_objects_response = _object_storage_client.list_objects(namespace_name=_namespace_name, bucket_name=_bucket_name)
    return len(list_objects_response.data.objects)

def getObjectsInBucket(_object_storage_client, _namespace_name, _bucket_name):
    # List the objects in the bucket
    list_objects_response = _object_storage_client.list_objects(namespace_name=_namespace_name, bucket_name=_bucket_name)
    return list_objects_response.data.objects

def getObjectNamesInBucket(_object_storage_client, _namespace_name, _bucket_name):
    bucket_objects = getObjectsInBucket(_object_storage_client, _namespace_name, _bucket_name)
    names = []
    for obj in bucket_objects:
        names.append(obj.name)
    return names

def upload_to_object_storage(_config, _namespace, _bucket, _path, _sema):
    """
    upload_to_object_storage will upload a file to an object storage bucket.
    This function is intended to be run as a separate process.  The client is
    created with each invocation so that the separate processes do
    not have a reference to the same client.

    :param config: a configuration dictionary used to create ObjectStorageClient
    :param namespace: Namespace where the bucket resides
    :param bucket: Name of the bucket in which the object will be stored
    :param path: path to file to upload to object storage
    :rtype: None
    """
    with open(_path, "rb") as in_file:
        name = os.path.basename(_path)
        ostorage = oci.object_storage.ObjectStorageClient(_config)
        ostorage.put_object(_namespace,
                            _bucket,
                            name,
                            in_file)
        # print("Finished uploading {}".format(name))
    _sema.release()

def getAllDatasetNames(_dl_client, _compartment_id):
    list_datasets_response = _dl_client.list_datasets(
        compartment_id=_compartment_id
    )
    dataset_names = []
    for dataset in list_datasets_response.data.items:
        dataset_names.append(dataset.display_name)
    return dataset_names

def getDatasetID(_dl_client, _compartment_id, _dataset_name):
    list_datasets_response = _dl_client.list_datasets(
        compartment_id=_compartment_id
    )
    for dataset in list_datasets_response.data.items:
        if dataset.display_name == _dataset_name:
            return dataset.id

def getDatasetState(_dl_client, _compartment_id, _dataset_name):
    list_datasets_response = _dl_client.list_datasets(
        compartment_id=_compartment_id
    )
    for dataset in list_datasets_response.data.items:
        if dataset.display_name == _dataset_name:
            return dataset.lifecycle_state

def downloadAllObjectsFromBucket(_dst_dir, _obj_storage_client, _compartment_id, _bucket_name):
    namespace = _obj_storage_client.get_namespace().data

    list_objects = getObjectsInBucket(_obj_storage_client, namespace, _bucket_name)

    for obj in tqdm(list_objects, desc="Downloading objects"):
        object_name = obj.name
        object_content = _obj_storage_client.get_object(
            namespace_name=namespace,
            bucket_name=_bucket_name,
            object_name=object_name
        ).data.content

        # Create a local file with the same name as the object and write the content
        local_file_path = os.path.join(_dst_dir, object_name)
        with open(local_file_path, 'wb') as local_file:
            local_file.write(object_content)

def init_dls_cp_client(_config, _service_endpoint):
    dls_client = DataLabelingManagementClient(_config,
                                              service_endpoint=_service_endpoint)
    return dls_client

def init_dls_dp_client(_config, _service_endpoint):
    dls_client = DataLabelingClient(_config,
                                    service_endpoint=_service_endpoint)
    return dls_client

def createDatasetFromBucket(_object_storage_client, _config, _compartment_id, _namespace, _bucket):
    # Init dataset settings
    format_type = "IMAGE"
    annotation_format = "BOUNDING_BOX"
    label1 = "tooltip"
    label2 = "zack"
    display_name = "delete_this_test_jpeg"
    dls_cp_client = oci.data_labeling_service.DataLabelingManagementClient(_config)
    dls_dp_client = oci.data_labeling_service_dataplane.DataLabelingClient(_config)
    dataset_source_details_obj = ObjectStorageSourceDetails(namespace=_namespace, bucket=_bucket)
    dataset_format_details_obj = DatasetFormatDetails(format_type=format_type)
    label_set_obj = LabelSet(items=[Label(name=label1), Label(name=label2)])

    # make sure dataset name isn't already taken
    dataset_names = getAllDatasetNames(dls_cp_client, _compartment_id)
    if display_name not in dataset_names:
        create_dataset_obj = CreateDatasetDetails(
            compartment_id=_compartment_id, 
            annotation_format=annotation_format,
            dataset_source_details=dataset_source_details_obj,
            dataset_format_details=dataset_format_details_obj,
            label_set=label_set_obj,
            display_name=display_name)

        response = dls_cp_client.create_dataset(create_dataset_details=create_dataset_obj)
        dataset_id = response.data.id

        # Wait for the dataset to be in active state
        print("Waiting for dataset to finish creating...")

        
    else:
        dataset_id = getDatasetID(dls_cp_client, _compartment_id, display_name)

    
    # Wait for the dataset to be in active state
    dataset_state = 'CREATING'
    while (dataset_state != 'ACTIVE'):
        dataset_state = getDatasetState(dls_cp_client, _compartment_id, display_name)
        time.sleep(0.1)
    print("Done creating!")

    # Generate records
    generate_record_obj = GenerateDatasetRecordsDetails(limit=BATCH_SIZE)
    dls_cp_client.generate_dataset_records(
        dataset_id=dataset_id,
        generate_dataset_records_details=generate_record_obj
    )
    # object_names = getObjectNamesInBucket(_object_storage_client, _namespace, _bucket)
    # for name in tqdm(object_names, desc=f"Generating records for dataset {display_name}"):
    #     source_details_obj = CreateObjectStorageSourceDetails(relative_path=".")
    #     create_record_obj = CreateRecordDetails(name=name,
    #                                             dataset_id=dataset_id,
    #                                             compartment_id=_compartment_id,
    #                                             source_details=source_details_obj)
    #     dls_dp_client.create_record(create_record_details=create_record_obj)


def checkNumThreadsAndCPUs():
    total_threads = psutil.cpu_count()/psutil.cpu_count(logical=False)
    print('You can run {} processes per CPU core simultaneously'.format(total_threads))
    total_cpu = mp.cpu_count()
    print(f"You have {total_cpu} CPU accessible")

if __name__ == "__main__":
    main()