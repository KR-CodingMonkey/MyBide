# import the necessary packages
from imutils import face_utils
from collections import OrderedDict

import numpy as np
import argparse
import imutils
import dlib
import cv2

FACIAL_LANDMARKS_68_IDXS = OrderedDict([
	("mouth", (48, 68)),
	#("inner_mouth", (60, 68)),
	("right_eyebrow", (17, 22)),
	("left_eyebrow", (22, 27)),
	("right_eye", (36, 42)),
	("left_eye", (42, 48)),
	("nose", (27, 36)),
	#("jaw", (0, 17))
])

FACIAL_LANDMARKS_IDXS = FACIAL_LANDMARKS_68_IDXS

def My_Visualize_Facial_Landmarks(image, shape, colors=None, alpha=0.75):
	# create two copies of the input image -- one for the
	# overlay and one for the final output image
	overlay = image.copy()
	output = image.copy()

	# if the colors list is None, initialize it with a unique
	# color for each facial landmark region
	if colors is None:
		colors = [(0, 0, 0), (0, 0, 0),(0, 0, 0),(0, 0, 0),(0, 0, 0),(0, 0, 0),(0, 0, 0),(0, 0, 0),]
	
	## loop over the facial landmark regions individually
	#for (i, name) in enumerate(FACIAL_LANDMARKS_IDXS.keys()):
	#	# grab the (x, y)-coordinates associated with the
	#	# face landmark
	#	(j, k) = FACIAL_LANDMARKS_IDXS[name]
	#	pts = shape[j:k]

	#	# since the jawline is a non-enclosed facial region,
	#	# just draw lines between the (x, y)-coordinates
	#	# otherwise, compute the convex hull of the facial
	#	# landmark coordinates points and display it
	#	hull = cv2.convexHull(pts)
	#	cv2.drawContours(overlay, [hull], -1, colors[i], -1)

	#pts = shape[1:66]
	pts = shape[18:66]
	hull = cv2.convexHull(pts)
	cv2.drawContours(overlay, [hull], -1, colors[i], -1)

	# apply the transparent overlay
	cv2.addWeighted(overlay, alpha, output, 1 - alpha, 0, output)

	# return the output image
	return output

# construct the argument parser and parse the arguments
#ap = argparse.ArgumentParser()
#ap.add_argument("-p", "--shape-predictor", required=True, help="path to facial landmark predictor")
#ap.add_argument("-i", "--image", required=True,	help="path to input image")
#args = vars(ap.parse_args())

# initialize dlib's face detector (HOG-based) and then create
# the facial landmark predictor
detector = dlib.get_frontal_face_detector()
predictor = dlib.shape_predictor("./shape_predictor_68_face_landmarks.dat")
#predictor = dlib.shape_predictor(args["shape_predictor"])

# load the input image, resize it, and convert it to grayscale
image = cv2.imread("./test11.png")
#image = cv2.imread(args["image"])
image = imutils.resize(image, width=500)
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# detect faces in the grayscale image
rects = detector(gray, 1)

# loop over the face detections
for (i, rect) in enumerate(rects):
	# determine the facial landmarks for the face region, then
	# convert the landmark (x, y)-coordinates to a NumPy array
	shape = predictor(gray, rect)
	shape = face_utils.shape_to_np(shape)

	# visualize all facial landmarks with a transparent overlay
	output = My_Visualize_Facial_Landmarks(image, shape, alpha=1.0)
	cv2.imshow("Image", output)
	cv2.waitKey(0)

