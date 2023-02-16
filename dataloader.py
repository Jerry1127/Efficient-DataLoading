from torch.utils.data import DataLoader
import threading
from queue import Queue
from dataset import ChunkDataset
import torch


class BackgroundGenerator(threading.Thread):
    def __init__(self, generator):
        super(BackgroundGenerator, self).__init__()
        self.queue = Queue()
        self.generator = generator
        self.daemon = True
        self.start()

    def run(self):
        for item in self.generator:
            self.queue.put(item)
        self.queue.put(None)

    def next(self):
        """
        batch_size大小 的数据在这里聚合
        :return:
        """
        images = self.queue.get()
        if images is None:
            raise StopIteration
        return images

    def __next__(self):
        return self.next()

    def __iter__(self):
        return self


class DataLoaderX(DataLoader):
    def __init__(self, *args, **kwargs):
        super(DataLoaderX, self).__init__(*args, **kwargs)
        # self.stream = torch.cuda.Stream()

    def __iter__(self):
        self.iter = super(DataLoaderX, self).__iter__()
        self.iter = BackgroundGenerator(self.iter)
        self.preload()
        return self

    def preload(self):
        self.batch = next(self.iter, None)
        if self.batch is None:
            return None
        # with torch.cuda.stream(self.stream):
        #     for k in range(len(self.batch)):
        #         self.batch[k] = self.batch[k].to(non_blocking=True)

    def __next__(self):
        # torch.cuda.current_stream().wait_stream(self.stream)
        batch = self.batch
        if batch is None:
            raise StopIteration
        self.preload()
        return batch


