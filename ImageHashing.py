from imagededup.methods import PHash, DHash, AHash
from imagededup.utils import plot_duplicates
import os
import shutil
from datetime import datetime

# IMAGE_DIR = "E:\\Videos\\small_temp_raw_frames"
SRC_DIR = "C:\\Users\\kkam\\Desktop\\filtered_frames\\10_3_23_hamm_15\\filter_bucket"
DEST_DIR = "C:\\Users\\kkam\\Desktop\\filtered_frames\\10_3_23_hamm_15\\more_filtered"
BATCH_SIZE = 10000
HAMM_DISTANCE = 15

def main():    
    finished = False
    src_dir = SRC_DIR
    startTime = datetime.now()
    while not finished:
        # Make sure dir has less than 10k images
        num_of_folders = breakFolderIntoBatches(src_dir, DEST_DIR, BATCH_SIZE)

        # Filter through all batch folders
        for x in range(num_of_folders):
            raw_batch_folder = os.path.join(DEST_DIR, str(x))
            filtered_batch_folder = os.path.join(DEST_DIR, str(x) + "_filtered")
            if not os.path.exists(filtered_batch_folder):
                os.makedirs(filtered_batch_folder)

            findUniqueImages(raw_batch_folder, filtered_batch_folder)
            print("Filtered batch ", x)

        # Filter images among many folders into one folder
        filterBucket = consolidateFolders(DEST_DIR, num_of_folders, src_dir)
        print("Consolidated")

        # If bucket is below batchsize, do one final filter
        if BATCH_SIZE > numOfFilesInDir(filterBucket):
            print("Final filter")
            # Make filter bucket
            final_bucket = os.path.join(DEST_DIR, "final")
            if not os.path.exists(final_bucket):
                os.makedirs(final_bucket)
            # Filter one last time
            findUniqueImages(filterBucket, final_bucket)
            moveFiles(filterBucket, SRC_DIR)
            shutil.rmtree(filterBucket)
            finished = True
        else:
            src_dir = filterBucket
            print("Filtering again")
    print(f"Took {datetime.now() - startTime} to run")

def findUniqueImages(_srcDir, _dstDir):
    """Uses PHash to filter _srcDir images and moves them to _dstDir"""
    # Import perceptual hashing method
    phasher = PHash()

    # Generate encodings
    encodings = phasher.encode_images(image_dir=_srcDir)
    # Find duplicates using the generated encodings
    duplicates = phasher.find_duplicates(encoding_map=encodings, 
                                         search_method='brute_force', 
                                         max_distance_threshold = HAMM_DISTANCE)

    seen = set()
    # Iterate over all files in dict
    for key in duplicates:
        # Check that file hasn't been seen yet
        if key not in seen:
            # Iterate through each duplicate img and mark as seen
            similar_images = duplicates[key]
            for img in similar_images:
                if img not in seen:
                    seen.add(img)
            # Move unique img to other dir
            os.rename(os.path.join(_srcDir,key), os.path.join(_dstDir,key))

def breakFolderIntoBatches(_srcDir, _dstDir, _batchSize):
    """Moves files from _srcDir into batches in _dstDir"""
    # Make sure dir has less than 10k images
    num_files = numOfFilesInDir(_srcDir)
    num_of_folders = 0
    while num_files > 0:
        batch_folder = os.path.join(_dstDir, str(num_of_folders))
        # Create temp folder to hold batch
        if not os.path.exists(batch_folder):
            os.makedirs(batch_folder)
        # Move 10,000 files into batch folder
        for filename, x in zip(os.listdir(_srcDir), range(_batchSize)):
            os.rename(os.path.join(_srcDir, filename), os.path.join(batch_folder, filename))
            if x == _batchSize:
                break
        num_of_folders += 1
        num_files -= _batchSize
    return num_of_folders

def consolidateFolders(_parentDir, _numOfFolders, _extraFolder):
    """Move filtered images into one location. Move extra images back to _extraFolder"""
    # Make filter bucket
    filter_bucket = os.path.join(DEST_DIR, "filter_bucket")
    if not os.path.exists(filter_bucket):
        os.makedirs(filter_bucket)
    # Iterate over all filter folders and move images to bucket
    for x in range(_numOfFolders):
        filtered_batch_folder = os.path.join(_parentDir, str(x) + "_filtered")
        for filename in os.listdir(filtered_batch_folder):
            os.rename(os.path.join(filtered_batch_folder, filename), os.path.join(filter_bucket, filename))
        # Cleanup folders
        shutil.rmtree(filtered_batch_folder)
        batch_folder = os.path.join(_parentDir, str(x))
        for filename in os.listdir(batch_folder):
            os.rename(os.path.join(batch_folder, filename), os.path.join(_extraFolder, filename))
        shutil.rmtree(batch_folder)
 
    return filter_bucket

def moveFiles(_srcDir, _dstDir):
    for filename in os.listdir(_srcDir):
        os.rename(os.path.join(_srcDir, filename), os.path.join(_dstDir, filename))

def numOfFilesInDir(_srcDir):
    return len([name for name in os.listdir(_srcDir) if os.path.isfile(os.path.join(_srcDir, name))])

if __name__ == "__main__":
    main()