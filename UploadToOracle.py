import os
import oci
import glob
import multiprocessing as mp
import math
from tqdm import tqdm
import json
import psutil

from oci.data_labeling_service_dataplane.data_labeling_client import DataLabelingClient
from oci.data_labeling_service.data_labeling_management_client import DataLabelingManagementClient
from oci.data_labeling_service.models import ObjectStorageSourceDetails
from oci.data_labeling_service.models import DatasetFormatDetails
from oci.data_labeling_service.models import LabelSet
from oci.data_labeling_service.models import Label
from oci.data_labeling_service.models import CreateDatasetDetails

IMAGE_DIR = "F:\\kyle_files\\Tip_Tracking_Stuff"
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
    file_search_string = os.path.join(IMAGE_DIR, "**/*.jpg")
    all_files = []
    for filename in glob.iglob(file_search_string, recursive=True):
        # print(filename)
        all_files.append(filename)
    num_files = len(all_files)

    # Create proper amount of buckets
    num_buckets = int(math.ceil(float(num_files)/BATCH_SIZE))
    buckets = []
    existing_buckets = getAllBucketNames(object_storage_client, namespace_name, compartment_id)

    # Check that bucket does not already exist
    for bucket_num in range(num_buckets):
        bucket_num += 1
        bucket_name = "batch_" + f"{bucket_num:03d}"
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

    # # Uncomment this to delete objects from bucket
    # deleteAllObjectsInBucket(object_storage_client, namespace_name, buckets[4])
    # exit()

    # # Upload images
    # for i in range(3,num_buckets):
    #     start_idx = BATCH_SIZE*i
    #     end_idx = BATCH_SIZE*(i+1) if BATCH_SIZE*(i+1) < num_files else num_files-1 # ensures that we don't go out of range
    #     filebatch = all_files[start_idx:end_idx]
    #     parallelUpload(filebatch, buckets[i], namespace_name, config, sema)

    # Find which images are missing
    missed = {}
    for i in range(num_buckets):
        start_idx = BATCH_SIZE*i
        end_idx = BATCH_SIZE*(i+1) if BATCH_SIZE*(i+1) < num_files else num_files-1 # ensures that we don't go out of range
        filebatch = all_files[start_idx:end_idx]

        # Verify that all files were uploaded
        objects = getObjectNamesInBucket(object_storage_client, namespace_name, buckets[i])
        missed[buckets[i]] = []
        for file_name in filebatch:
            if os.path.basename(file_name) not in objects:
                missed[buckets[i]].append(file_name)
        print(f"Files missed: '{len(missed[buckets[i]])}'")

    # Write missed files to json
    with open(MISSED_FILES_JSON, "w") as outfile:
        outfile.write(json.dumps(missed, indent=4))

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

def init_dls_cp_client(_config, _service_endpoint):
    dls_client = DataLabelingManagementClient(_config,
                                              service_endpoint=_service_endpoint)
    return dls_client

def init_dls_dp_client(_config, _service_endpoint):
    dls_client = DataLabelingClient(_config,
                                    service_endpoint=_service_endpoint)
    return dls_client

def createDatasetFromBucket(_config, _compartment_id, _namespace, _bucket):
    format_type = "IMAGE"
    annotation_format = "BOUNDING_BOX"
    label1 = "tooltip"
    label2 = "zack"
    
    service_endpoint_cp = "https://dlsprod-cp.us-phoenix-1.oci.oraclecloud.com"
    dls_client = init_dls_cp_client(_config, service_endpoint_cp)
    
    dataset_source_details_obj = ObjectStorageSourceDetails(namespace=_namespace, bucket=_bucket)
    dataset_format_details_obj = DatasetFormatDetails(format_type=format_type)
    label_set_obj = LabelSet(items=[Label(name=label1), Label(name=label2)])
    create_dataset_obj = CreateDatasetDetails(compartment_id=_compartment_id, annotation_format=annotation_format,
                                            dataset_source_details=dataset_source_details_obj,
                                            dataset_format_details=dataset_format_details_obj,
                                            label_set=label_set_obj)
    try:
        response = dls_client.create_dataset(create_dataset_details=create_dataset_obj)
    except Exception as error:
        response = error

def checkNumThreadsAndCPUs():
    total_threads = psutil.cpu_count()/psutil.cpu_count(logical=False)
    print('You can run {} processes per CPU core simultaneously'.format(total_threads))
    total_cpu = mp.cpu_count()
    print(f"You have {total_cpu} CPU accessible")

if __name__ == "__main__":
    main()