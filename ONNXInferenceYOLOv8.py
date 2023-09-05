import cv2
import numpy as np
import onnxruntime as ort
import VideoUtils
from PIL import Image

# VIDEO_PATH = "F:\\kyle_files\\repos\\RefineImageDataset\\used_videos\\4828b9ac-f90f-4dc7-bab0-5264dcdbf1f5.avi"
VIDEO_PATH = "C:\\Users\\kkam\\repos\\kyle_python_scripts\\videos\\Default_Surgeon_Jun_02,_2023_08.48.21_AM.avi"
MODEL_PATH = ".\\models\\tool_tip_v4.onnx"
OUTPUT_PATH = ".\\videos\\Default_Surgeon_Jun_02,_2023_08.48.21_AM_PROCESSED.mp4"
CONF_THRESHOLD = 0.5
IOU_THRESHOLD = 0.5

def calculate_iou(box1, box2):
    """
    Calculate Intersection over Union (IoU) of two bounding boxes.
    :param box1: First bounding box (x, y, width, height)
    :param box2: Second bounding box (x, y, width, height)
    :return: Intersection over Union (IoU) value
    """
    x1, y1, w1, h1 = box1
    x2, y2, w2, h2 = box2

    # Calculate coordinates of intersection rectangle
    intersection_x = max(x1, x2)
    intersection_y = max(y1, y2)
    intersection_w = max(0, min(x1 + w1, x2 + w2) - intersection_x)
    intersection_h = max(0, min(y1 + h1, y2 + h2) - intersection_y)

    # Calculate area of intersection rectangle
    intersection_area = intersection_w * intersection_h

    # Calculate area of both bounding boxes
    box1_area = w1 * h1
    box2_area = w2 * h2

    # Calculate Intersection over Union (IoU)
    iou = intersection_area / float(box1_area + box2_area - intersection_area)
    return iou

def perform_nms(boxes, scores, iou_threshold):
    """
    Perform Non-Maximum Suppression (NMS) on bounding boxes.
    :param boxes: List of bounding boxes (x, y, width, height)
    :param scores: List of corresponding confidence scores
    :param iou_threshold: IoU threshold for NMS
    :return: List of selected bounding boxes after NMS
    """
    selected_boxes = []

    # Sort bounding boxes by confidence scores in descending order
    sorted_indices = np.argsort(scores)[::-1]

    while len(sorted_indices) > 0:
        # Select the bounding box with the highest confidence score
        best_index = sorted_indices[0]
        selected_boxes.append(boxes[best_index])

        # Calculate IoU of the selected box with the remaining boxes
        ious = [calculate_iou(boxes[best_index], boxes[i]) for i in sorted_indices[1:]]

        # Remove boxes with IoU greater than the threshold
        remaining_indices = []
        for i, iou in enumerate(ious):
            if iou <= iou_threshold:
                remaining_indices.append(sorted_indices[i + 1])
        sorted_indices = remaining_indices

    return selected_boxes

def initONNX():
    # Set the CUDA device if available
    cuda_device = 'cuda:0'  # Change to the desired CUDA device ID, e.g., 'cuda:1'

    print(ort.get_device())
    print(ort.get_available_providers())

    # Enable CUDA execution
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1
    options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    options.enable_profiling = False
    options.log_severity_level = 3  # Suppress ONNX Runtime log messages

    if cuda_device is not None:
        print("Using GPU")
        options.add_session_config_entry('session.use_cuda', '1')
        options.add_session_config_entry('session.gpu_device_id', cuda_device)
    else:
        print("Using CPU")

    providers = [("CUDAExecutionProvider", {"cudnn_conv_use_max_workspace": '1'})]

    session = ort.InferenceSession(MODEL_PATH, options, providers=providers)
    assert 'CUDAExecutionProvider' in session.get_providers(), "CUDAExecutionProvider not found"
    print("Session initialized")

    return session

def main():
    # Initialzie ONNX with CUDA
    session = initONNX()

    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name

    # Open the video file
    cap = cv2.VideoCapture(VIDEO_PATH)
    print("Opened video ", VIDEO_PATH)

    # Check if the video file opened successfully
    if not cap.isOpened():
        print("Error opening video file")
        return

    # Read video properties
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    if VideoUtils.isStereo(frame_width, frame_height):
        frame_width /= 2 # monoscopic
    fps = cap.get(cv2.CAP_PROP_FPS)

    # Create video writer
    output_video = cv2.VideoWriter(OUTPUT_PATH, 0x7634706d, fps, (int(frame_width), frame_height))

    # Process video frames
    count = 0
    length = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    while cap.isOpened():
        ret, frame = cap.read()
        
        # Break loop if no more frames are available
        if not ret:
            break

        # Preprocess the frame
        # mono_left_half = int(frame.shape[1]/2)
        # frame = frame[:,0:mono_left_half]
        frame = VideoUtils.getLeftImage(frame)
        tempImage = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
        input_data = cv2.resize(frame, (640, 640))
        input_data = np.expand_dims(input_data, axis=0)
        input_data = np.transpose(input_data, (0, 3, 1, 2))
        input_data = input_data.astype(np.float32)

        # Run object detection on the frame
        outputs = session.run([output_name], {input_name: input_data})
        detections = outputs[0][0]
        tempArray = np.array(detections)

        # Post-process the detections
        num_classes = detections.shape[0]-4
        det_mat = np.array(detections)
        det_mat = np.transpose(det_mat)
        x_factor = frame_width/640
        y_factor = frame_height/640

        # Iterate through each column

        confs = []
        boxes = []
        for i in range(0,det_mat.shape[0]): #iterate through all 1000+ predictions
            for j in range(0,det_mat.shape[1]): # iterate through each class prediction
                temp1 = det_mat.shape[0]
                temp2 = det_mat.shape[1]
                class_id = 0
                if det_mat[i][-1:] > CONF_THRESHOLD:
                    confs.append(det_mat[i][-1:])

                    x = det_mat[i][0]
                    y = det_mat[i][1]
                    w = det_mat[i][2]
                    h = det_mat[i][3]

                    width = w * x_factor
                    height = h * y_factor

                    topLeft_x = int((x - (0.5 * w)) * x_factor)
                    topLeft_y = int((y - (0.5 * h)) * y_factor)
                    boxes.append([topLeft_x,topLeft_y,width,height])
        
        if (len(boxes) > 0):
            print("Found tool tips: ", len(boxes))

        # NMS
        nms_boxes = perform_nms(boxes, confs, IOU_THRESHOLD)

        # Draw bounding boxes on the frame
        for detection in nms_boxes:
            x, y, w, h = detection[:4]
            cv2.rectangle(frame, (x, y), (x+w, y+h), (0, 255, 0), 2)

        # Write the processed frame to the output video file
        output_video.write(frame)

        # Display the frame (optional)
        cv2.resize(frame, (int(1920/2),int(1080/2)))
        cv2.imshow("Video", frame)
        print("Finished frame num: ", count, " / ", length)
        count += 1
        # Wait for a key press to exit (optional)
        if cv2.waitKey(1) == 27:  # Press Esc to exit
            break

    # Release the video capture and writer, and close the window
    cap.release()
    output_video.release()
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()