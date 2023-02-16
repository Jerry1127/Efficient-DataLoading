import os
import random

from PIL import Image
from io import BytesIO
from multiprocessing import pool
import json


class ChunkFile:
    """
    块文件
    """

    def __init__(self):
        self.total = 0
        self.offsets = []
        self.sizes = []
        self.labels = []
        self.values = []

    def show_all(self):
        """
        展示所有图片
        :return:
        """
        for i in range(self.total):
            print(self.labels[i])
            img = Image.open(BytesIO(self.values[i]))
            img.show()

    def __getitem__(self, index):
        return self.values[index], self.labels[index]

    def __len__(self):
        return self.total


def make_chunk(files: list, filename):
    """
    将多个文件读入到一个块中
    :files: in:文件列表
    :filename: out:chunk文件名
    :return: 布尔值：失败或者成功
    """
    # 子文件个数，
    total = len(files)
    offsets = []
    sizes = []
    labels = []
    start = (3 * total + 1) * 4
    try:
        with open(filename, "wb") as chunk:
            chunk.seek(start, 0)

            for file, label in files:
                offsets.append(start)
                size = int(os.path.getsize(file))
                sizes.append(size)
                labels.append(label)
                with open(file, "rb") as f:
                    chunk.write(f.read())
                start += size
                chunk.seek(start, 0)
            # 写入元数据
            chunk.seek(0, 0)
            chunk.write(total.to_bytes(4, "big", signed=False))
            for offset in offsets:
                chunk.write(offset.to_bytes(4, "big", signed=False))
            for size in sizes:
                chunk.write(size.to_bytes(4, "big", signed=False))
            for label in labels:
                chunk.write(label.to_bytes(4, "big", signed=False))
    except FileNotFoundError:
        return False

    return True


def read_seq_chunk(chunk_file_name):
    """
    读取整个块文件
    :return: ChunkFile
    """
    chunkFile = ChunkFile()
    with open(chunk_file_name, "rb") as chunk:
        total = int.from_bytes(chunk.read(4), "big")
        chunkFile.total = total
        chunk.seek(4, 0)
        metadata = chunk.read((3 * total) * 4)
        i = 0
        while i < total * 4:
            chunkFile.offsets.append(int.from_bytes(metadata[i:i + 4], "big"))
            i += 4
        while i < total * 8:
            chunkFile.sizes.append(int.from_bytes(metadata[i:i + 4], "big"))
            i += 4
        while i < total * 12:
            chunkFile.labels.append(int.from_bytes(metadata[i:i + 4], "big"))
            i += 4

        for i in range(total):
            chunk.seek(chunkFile.offsets[i], 0)
            chunkFile.values.append(chunk.read(chunkFile.sizes[i]))

    return chunkFile


def read_random_chunk(chunk_file_name, location):
    """
    随机读取块中的任意位置的一个文件
    :param chunk_file_name: 块文件路径
    :param location: 个体文件在块中的路径
    :return: 单个文件的二进制

    example:

    """
    with open(chunk_file_name, "rb") as chunk:
        total = int.from_bytes(chunk.read(4), "big")
        if location > total or location < 1:
            raise ValueError("当前访问的元素已经超出了块大小,此块的实际大小为1-{}".format(total))
        chunk.seek(0, 0)
        metadata = chunk.read((3 * total + 1) * 4)
        offset = int.from_bytes(metadata[location * 4:(location + 1) * 4], "big")
        size = int.from_bytes(metadata[(location + total) * 4:(location + total + 1) * 4], "big")
        label = int.from_bytes(metadata[(location + 2 * total) * 4:(location + 2 * total + 1) * 4], "big")
        chunk.seek(offset, 0)
        res = chunk.read(size)
    return res, label


def make_chunks(root: str, chunk_size: int, save_path="", num_works=0):
    """
    只需要输入需要做块的文件根目录，就可以将该目录下的所有文件做成块，并且每个文件所在的目录默认为该文件的标签
    :param root: 文件根目录
    :param chunk_size: 块大小
    :param num_works: 工作进程，默认为0
    :return:
    """
    classes = [d.name for d in os.scandir(root) if d.is_dir()]
    classes.sort()
    classes_to_idxs = {class_name: i for i, class_name in enumerate(classes)}
    paths = []
    metadata = {}
    metadata["classes_to_idxs"] = classes_to_idxs

    if len(save_path) != 0 and not os.path.exists(save_path):
        os.mkdir(save_path)

    for dir in os.scandir(root):
        if not dir.is_dir():
            continue
        label = classes_to_idxs[dir.name]
        for file in os.listdir(dir):
            paths.append((os.path.join(dir, file), label))
    random.shuffle(paths)

    def list_of_groups(init_list, childern_list_len):
        list_of_groups = zip(*(iter(init_list),) * childern_list_len)
        end_list = [list(i) for i in list_of_groups]
        count = len(init_list) % childern_list_len
        end_list.append(init_list[-count:]) if count != 0 else end_list
        return end_list

    path_lists = list_of_groups(paths, chunk_size)
    metadata["total"] = len(paths), len(path_lists[0])
    # chunk_dict = {}
    if num_works == 0:
        for i in range(len(path_lists)):
            # achunk = {j: apath for j, apath in enumerate(path_lists[i])}
            make_chunk(path_lists[i], os.path.join(save_path, "chunk{}.mimg".format(i+1)))
            # chunk_dict["chunk{}".format(i)] = achunk
    else:
        p = pool.Pool(num_works)
        for i in range(len(path_lists)):
            # achunk = {j: apath for j, apath in enumerate(path_lists[i])}
            p.apply_async(make_chunk, args=(path_lists[i], os.path.join(save_path, "chunk{}.mimg".format(i+1))))
            # chunk_dict["chunk{}".format(i)] = achunk
        p.close()
        p.join()
    # metadata["chunk_dict"] = chunk_dict
    metadata_str = json.dumps(metadata)
    with open(os.path.join(save_path, "metadata.json"), "w") as json_file:
        json_file.write(metadata_str)


if __name__ == '__main__':
    make_chunks("/mnt/storage/dataset/train",32,"/mnt/storage/dataset/ILSVRC2012_dataset/new_train_pack_32",16)
