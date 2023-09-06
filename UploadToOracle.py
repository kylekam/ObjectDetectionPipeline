import os
import oci
import subprocess
import glob
from multiprocessing import Process
import math
from tqdm import tqdm

IMAGE_DIR = "F:\\kyle_files\\Tip_Tracking_Stuff"

BATCH_SIZE = 1000


# oci os object bulk-upload --namespace idrvtcm33fob --bucket-name ???? --src-dir ????

def main():
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

    # deleteAllObjectsInBucket(object_storage_client, namespace_name, buckets[0])
    # exit()

    # Upload images
    missed = {}
    for i in range(num_buckets):
        # filebatch = all_files[BATCH_SIZE*i:BATCH_SIZE*(i+1)]
        filebatch = all_files[BATCH_SIZE*i:BATCH_SIZE*(i+1)]
        parallelUpload(filebatch, buckets[i], namespace_name, config)
        
        # Verify that all files were uploaded
        objects = getObjectNamesInBucket(object_storage_client, namespace_name, buckets[0])
        missed[buckets[i]] = []
        for file_name in filebatch:
            if os.path.basename(file_name) not in objects:
                missed[buckets[i]].append(file_name)
        print(f"Files missed: '{missed[buckets[i]]}'")
        break



    # 5. Bonus: Create excel sheet

def parallelUpload(_src_list: list, _dst_bucket: str, _namespace_name: str, _config):
    """
    parallelUpload will 
    """

    # print("Starting upload for {}".format(_dst_bucket))
    proc_list = []
    for file_path in _src_list:
        # print("Starting upload for {}".format(file_path))
        p = Process(target=upload_to_object_storage, args=(_config,
                                                        _namespace_name,
                                                        _dst_bucket,
                                                        file_path))
        p.start()
        proc_list.append(p)

    # Upload files
    for job in tqdm(proc_list, desc=f"Uploading {_dst_bucket}"):
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

def upload_to_object_storage(config, namespace, bucket, path):
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
    with open(path, "rb") as in_file:
        name = os.path.basename(path)
        ostorage = oci.object_storage.ObjectStorageClient(config)
        ostorage.put_object(namespace,
                            bucket,
                            name,
                            in_file)
        # print("Finished uploading {}".format(name))

if __name__ == "__main__":
    main()