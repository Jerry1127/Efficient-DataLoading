import os

from torch.utils.data import Dataset
import json
import chunk
from PIL import Image
from io import BytesIO


class ChunkDataset(Dataset):

    def __init__(self, root: str, transform, ):
        self.root = root
        self.metadata = json.load(open(os.path.join(root, "metadata.json"), 'r'))
        self.total = self.metadata["total"][0]
        self.chunksize = self.metadata["total"][1]
        self.cache = dict()
        self.transform = transform

    def __getitem__(self, item):
        if item in self.cache.keys():
            data = self.cache[item]
            del self.cache[item]
        else:
            row = item // self.chunksize + 1
            col = item % self.chunksize + 1
            chunkFile = chunk.read_seq_chunk(os.path.join(self.root, "chunk{}.mimg".format(row)))
            for i in range(len(chunkFile)):
                self.cache[(row - 1) * self.chunksize + i] = chunkFile[i]
            data = self.cache[item]
            del self.cache[item]
        img = data[0]
        label = data[1]
        del data
        if self.transform is not None:
            img = self.transform(Image.open(BytesIO(img)).convert("RGB"))
        return img, label

    def __len__(self):
        return self.total
