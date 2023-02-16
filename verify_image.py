import os
import imghdr
from progressbar import ProgressBar

# path = '/mnt/storage/dataset/train'
path = '/home/shipc/datasets/ILSVRC2012_dataset/train'
original_images = []

for root, dirs, filenames in os.walk(path):
    for filename in filenames:
        original_images.append(os.path.join(root, filename))

original_images = sorted(original_images)
print('num:', len(original_images))
f = open('check_error.txt', 'w+')
error_images = []
progress = ProgressBar()

for filename in progress(original_images):
    # print(filename)
    check = imghdr.what(filename)
    # print(check)
    if check == None:
        f.write(filename)
        f.write('\n')
        error_images.append(filename)

print(len(error_images))
f.seek(0)

for s in f:
    print(s)

f.close()

