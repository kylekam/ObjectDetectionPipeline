import os
import sys
dir = "C:\\Users\\kkam\\repos\\Images\\delete_this_test_jpeg"
for filename in os.listdir(dir):
    infilename = os.path.join(dir,filename)
    if not os.path.isfile(infilename): continue
    oldbase = os.path.splitext(filename)
    newname = infilename.replace('.jpg', '.jpeg')
    output = os.rename(infilename, newname)