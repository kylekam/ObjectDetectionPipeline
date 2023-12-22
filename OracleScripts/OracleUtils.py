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

from oci.data_labeling_service.models import ObjectStorageSourceDetails
from oci.data_labeling_service.models import DatasetFormatDetails
from oci.data_labeling_service.models import LabelSet
from oci.data_labeling_service.models import Label
from oci.data_labeling_service.models import CreateDatasetDetails
from oci.data_labeling_service.models import GenerateDatasetRecordsDetails
from oci.data_labeling_service.models import SnapshotDatasetDetails
from oci.data_labeling_service.models import ObjectStorageSnapshotExportDetails
from oci.data_labeling_service.models import ExportFormat
from oci.object_storage.models import CreateBucketDetails
from oci.object_storage.models import CopyObjectDetails

class BucketHelpers():
    def __init__(self, _config):
        self.config = _config
        self.object_storage_client = oci.object_storage.ObjectStorageClient(_config)
        
    def doesBucketExist(self, _namespace_name, _bucket_name):
        try:
            # List the buckets in the compartment
            self.object_storage_client.get_bucket(namespace_name=_namespace_name, bucket_name=_bucket_name)
            print("Bucket {} exists".format(_bucket_name))
            return True
        except oci.exceptions.ServiceError as e:
            print("Bucket {} does not exist".format(_bucket_name))
            return False
        except Exception as e:
            print("Error: {}".format(e))
            return False

    def createBucket(self, _namespace_name, _compartment_id, _bucket_name):
        bucket_details = CreateBucketDetails(name=_bucket_name, compartment_id=_compartment_id)
        self.object_storage_client.create_bucket(namespace_name=_namespace_name, create_bucket_details=bucket_details)


    def parallelUpload(self, _src_list: list, _dst_bucket: str, _namespace_name: str, _sema):
        """
        parallelUpload will upload files concurrently to Oracle buckets.
        """

        # print("Starting upload for {}".format(_dst_bucket))
        proc_list = []
        for file_path in tqdm(_src_list, desc=f"Uploading {_dst_bucket}", colour="green"):
            _sema.acquire()
            # print("Starting upload for {}".format(file_path))
            p = mp.Process(target=self.upload_to_object_storage, args=(self.config,
                                                            _namespace_name,
                                                            _dst_bucket,
                                                            file_path,
                                                            _sema))
            p.start()
            proc_list.append(p)

        # Upload files
        for job in tqdm(proc_list, desc=f"Verifying {_dst_bucket}", colour="green"):
            job.join()


    def getAllBucketNames(self, _namespace_name, _compartment_id):
        # List the buckets in the compartment
        buckets = self.object_storage_client.list_buckets(namespace_name=_namespace_name, compartment_id=_compartment_id)

        bucket_list = []
        for bucket in buckets.data:
            bucket_list.append(bucket.name)

        return bucket_list


    def deleteAllObjectsInBucket(self, _namespace_name, _bucket_name):
        # List the objects in the bucket
        list_objects_response = self.object_storage_client.list_objects(namespace_name=_namespace_name, bucket_name=_bucket_name)

        # Iterate through the objects and delete them
        for obj in tqdm(list_objects_response.data.objects, desc=f"Deleting {_bucket_name} objects"):
            object_name = obj.name

            # Delete the object
            self.object_storage_client.delete_object(namespace_name=_namespace_name, bucket_name=_bucket_name, object_name=object_name)

    def deleteObject(self, _namespace_name, _bucket_name, _object_name):
        self.object_storage_client.delete_object(namespace_name=_namespace_name, bucket_name=_bucket_name, object_name=_object_name)

    def getNumOfObjectsInBucket(self, _namespace_name, _bucket_name):
        '''
        Returns number of objects in a bucket. Does not include DLS records.
        '''
        # List the objects in the bucket
        list_objects_response = self.object_storage_client.list_objects(namespace_name=_namespace_name, bucket_name=_bucket_name)
        
        # Do not count objects that are records
        count = 0
        for obj in list_objects_response.data.objects:
            if "ocid" not in obj.name:
                count += 1
        return count


    def getObjectsInBucket(self, _namespace_name, _bucket_name):
        # List the objects in the bucket
        list_objects_response = self.object_storage_client.list_objects(namespace_name=_namespace_name, bucket_name=_bucket_name)
        objects = [obj for obj in list_objects_response.data.objects]
        while list_objects_response.data.next_start_with != None:
            list_objects_response = self.object_storage_client.list_objects(namespace_name=_namespace_name, bucket_name=_bucket_name, start=list_objects_response.data.next_start_with)
            for obj in list_objects_response.data.objects:
                objects.append(obj)

        return objects


    def getObjectNamesInBucket(self, _namespace_name, _bucket_name):
        bucket_objects = self.getObjectsInBucket(_namespace_name, _bucket_name)
        names = []
        for obj in bucket_objects:
            names.append(obj.name)

        # while next_page != None:
        #     bucket_objects, next_page = self.getObjectsInBucket(_namespace_name, _bucket_name)
        #     for obj in bucket_objects:
        #         names.append(obj.name)
            
        return names


    def upload_to_object_storage(self, _namespace, _bucket, _path, _sema):
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
            mimetype, _ = mimetypes.guess_type(_path)
            name = os.path.basename(_path)
            self.object_storage_client.put_object(namespace_name=_namespace,
                                                  bucket_name=_bucket,
                                                  object_name=name,
                                                  content_type=mimetype,
                                                  put_object_body=in_file)
            # print("Finished uploading {}".format(name))
        _sema.release()

    def singleUpload(self, _src_path, _dst_bucket, _dst_name, _namespace_name):
        """
        Uploads a single file to a bucket.
        """
        with open(_src_path, "rb") as in_file:
            mimetype, _ = mimetypes.guess_type(_src_path)
            self.object_storage_client.put_object(namespace_name=_namespace_name,
                                                  bucket_name=_dst_bucket,
                                                  object_name=_dst_name,
                                                  content_type=mimetype,
                                                  put_object_body=in_file)


    def downloadAllObjectsFromBucket(self, _dst_dir, _compartment_id, _bucket_name):
        namespace = self.object_storage_client.get_namespace().data
        list_objects = self.getObjectsInBucket(self.object_storage_client, namespace, _bucket_name)

        for obj in tqdm(list_objects, desc="Downloading objects"):
            object_name = obj.name
            object_content = self.object_storage_client.get_object(
                namespace_name=namespace,
                bucket_name=_bucket_name,
                object_name=object_name
            ).data.content

            # Create a local file with the same name as the object and write the content
            local_file_path = os.path.join(_dst_dir, object_name)
            with open(local_file_path, 'wb') as local_file:
                local_file.write(object_content)

    def copyObject(self, _src_namespace, _src_bucket, _dst_bucket, _src_object_name, _dst_object_name):
        """
        Object will have the same name in the destination bucket.
        """
        copy_details = CopyObjectDetails(
            source_object_name=_src_object_name,
            destination_bucket=_dst_bucket,
            destination_namespace=_src_namespace,
            destination_object_name = _dst_object_name,
            destination_region=self.config['region']
        )

        self.object_storage_client.copy_object(namespace_name=_src_namespace,bucket_name=_src_bucket,copy_object_details=copy_details)

    def renameObject(self, _namespace_name, _bucket_name, _new_name, _old_name):
        self.object_storage_client.rename_object(
            namespace_name=_namespace_name,
            bucket_name=_bucket_name,
            rename_object_details=oci.object_storage.models.RenameObjectDetails(
                new_name = _new_name,
                source_name = _old_name
            )
        )


#-------------------------------------DATASET FUNCTIONS-------------------------------------#
class DatasetHelpers():
    def __init__(self, _config, _compartment_id):
        self.config = _config
        self.dlmp_client = oci.data_labeling_service.DataLabelingManagementClient(_config)
        self.dlc_client = oci.data_labeling_service_dataplane.DataLabelingClient(_config)


    # Define a function to list all dataset OCIDs in the current compartment
    def getDatasetID(self, _compartment_id, _dataset_name):
        list_datasets_response = self.dlmp_client.list_datasets(
            compartment_id=_compartment_id,
            limit=100
        )
        for dataset in list_datasets_response.data.items:
            if dataset.display_name == _dataset_name:
                return dataset.id


    def getDatasetState(self, _compartment_id, _dataset_name):
        list_datasets_response = self.dlmp_client.list_datasets(
            compartment_id=_compartment_id,
            limit=100
        )
        for dataset in list_datasets_response.data.items:
            if dataset.display_name == _dataset_name:
                return dataset.lifecycle_state


    def getAllDatasetNames(self, _compartment_id):
        list_datasets_response = self.dlmp_client.list_datasets(
            compartment_id=_compartment_id
        )
        dataset_names = []
        for dataset in list_datasets_response.data.items:
            dataset_names.append(dataset.display_name)
        return dataset_names


    def getDatasetOCIDs(self, _compartment_id):
        """
        Returns a list of OCIDs for each dataset in the compartment.
        """
        dataset_ocids = []

        # Create a request to list datasets in the current compartment
        datasets_list = self.dlmp_client.list_datasets(compartment_id=_compartment_id)

        # List datasets and collect their OCIDs
        try:
            for dataset in datasets_list.data.items:
                dataset_ocids.append(dataset.id)
        except oci.exceptions.ServiceError as e:
            print(f"Error listing datasets: {e}")

        return dataset_ocids


    def getDatasetNames(self, _compartment_id):
        """
        Returns a list of names for each dataset in the compartment.
        """
        dataset_names = []

        # Create a request to list datasets in the current compartment
        datasets_list = self.dlmp_client.list_datasets(compartment_id=_compartment_id)

        # List datasets and collect their OCIDs
        try:
            for dataset in datasets_list.data.items:
                dataset_names.append(dataset.display_name)
        except oci.exceptions.ServiceError as e:
            print(f"Error listing datasets: {e}")

        return dataset_names


    def deleteDatasets(self, _compartment_id):
        """
        Deletes all the datasets in the _compartment_id
        """
        dataset_ocids = self.getDatasetOCIDs(_compartment_id)
        dataset_names = self.getDatasetNames(_compartment_id)

        for ocid,display_name in tqdm(zip(dataset_ocids,dataset_names), desc="Deleting datasets:", colour="green"):
            dataset_state = self.getDatasetState(self.dlmp_client, _compartment_id, display_name)
            if dataset_state == "DELETED":
                continue
            self.dlmp_client.delete_dataset(ocid)
        return


    def createDatasetFromBucket(self, _object_storage_client, _compartment_id, _namespace, _bucket, _labels, _batch_size):
        # Init dataset settings
        format_type = "IMAGE"
        annotation_format = "BOUNDING_BOX"
        display_name = _bucket
        dataset_source_details_obj = ObjectStorageSourceDetails(namespace=_namespace, bucket=_bucket)
        dataset_format_details_obj = DatasetFormatDetails(format_type=format_type)
        label_set_obj = LabelSet(items=[Label(name=label) for label in _labels])

        # make sure dataset name isn't already taken
        dataset_names = self.getAllDatasetNames(_compartment_id)
        if display_name: #not in dataset_names:
            create_dataset_obj = CreateDatasetDetails(
                compartment_id=_compartment_id, 
                annotation_format=annotation_format,
                dataset_source_details=dataset_source_details_obj,
                dataset_format_details=dataset_format_details_obj,
                label_set=label_set_obj,
                display_name=display_name)

            response = self.dlmp_client.create_dataset(create_dataset_details=create_dataset_obj)
            dataset_id = response.data.id

            # Wait for the dataset to be in active state
            print("Waiting for dataset to finish creating...")
            
        else:
            dataset_id = self.getDatasetID(_compartment_id, display_name)

        
        # Wait for the dataset to be in active state
        # TODO: make the the dataset creation stall until the records are generated
        dataset_state = 'CREATING'
        while (((dataset_state != 'ACTIVE') and (dataset_state != 'FAILED')) or (dataset_state == 'UPDATING')):
            dataset_state = self.getDatasetState(_compartment_id, display_name)
            time.sleep(0.1)
        print("Done creating!")

        # Generate records
        generate_record_obj = GenerateDatasetRecordsDetails(limit=_batch_size)
        self.dlmp_client.generate_dataset_records(
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


    def exportDatasetToBucket(self, _dest_bucket: str, _compartment_id: str, _bucket_name: str, _namespace: str):
        snapshot_details = SnapshotDatasetDetails(
            are_annotations_included=True,
            are_unannotated_records_included=True,
            export_details = ObjectStorageSnapshotExportDetails(
                bucket=_dest_bucket,
                export_type="OBJECT_STORAGE",
                namespace=_namespace
            ),
            export_format=ExportFormat(name="YOLO", version="V5")
        )

        dataset_id = self.getDatasetID(_compartment_id, _bucket_name)

        response = self.dlmp_client.snapshot_dataset(dataset_id=dataset_id, snapshot_dataset_details=snapshot_details)

        dataset_state = 'UPDATING'
        while (self.getDatasetState(_compartment_id, _bucket_name) != 'UPDATING'):
            time.sleep(1)
        while (dataset_state == 'UPDATING'):
            dataset_state = self.getDatasetState(_compartment_id, _bucket_name)
            time.sleep(0.1)
        if dataset_state == 'NEEDS_ATTENTION':
            print("Error exporting dataset to bucket. Please check the dataset for errors.")
        

    def exportDatasetToBucket_Kyle(self, _bh, _dest_bucket: str, _src_compartment_id: str, _src_bucket: str, _namespace: str):
        '''
        Exports a dataset to a bucket. First snapshot the dataset, this will create annotations for all images that
        have labels. Then find the images without labels and export those. Create blank labels for those.
        '''
        snapshot_details = SnapshotDatasetDetails(
            are_annotations_included=True,
            are_unannotated_records_included=True,
            export_details = ObjectStorageSnapshotExportDetails(
                bucket=_dest_bucket,
                export_type="OBJECT_STORAGE",
                namespace=_namespace
            ),
            export_format=ExportFormat(name="YOLO", version="V5")
        )

        dataset_id = self.getDatasetID(_src_compartment_id, _src_bucket)

        print("Attempting to export dataset {} to bucket {}".format(_src_bucket, _dest_bucket))

        response = self.dlmp_client.snapshot_dataset(dataset_id=dataset_id, snapshot_dataset_details=snapshot_details)

        dataset_state = 'UPDATING'
        while (self.getDatasetState(_src_compartment_id, _src_bucket) != 'UPDATING'):
            time.sleep(1)
        while (dataset_state == 'UPDATING'):
            dataset_state = self.getDatasetState(_src_compartment_id, _src_bucket) # roughly 10 min per 500 images
            time.sleep(1)
        if dataset_state == 'NEEDS_ATTENTION':
            print("Error exporting dataset to bucket. Please check the dataset for errors.")