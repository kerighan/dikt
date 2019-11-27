from tqdm import tqdm
from enum import Enum
import zipfile
import shutil
import json
import os


__version__ = "0.0.1"


class Dikt(object):
    def __init__(self, filename):
        self.name = filename.split("/")[-1].split(".")[0]
        self.zipf = zipfile.ZipFile(filename)
        with self.zipf.open(self.name + "/config.txt") as f:
            data = f.read().decode("utf8")
            lines = data.splitlines()
            dtype = lines[0]
            self.max_len = int(lines[1])
            self.chunk_keys = tuple(key for key in lines[2:])
            self.chunks = len(self.chunk_keys) + 1

        if dtype == "int":
            self.dtype = int
        elif dtype == "float":
            self.dtype = float
        elif dtype == "list" or dtype == "dict":
            self.dtype = json.loads

    def get_chunk_from_key(self, key):
        for i, chunk_key in enumerate(self.chunk_keys):
            if key < chunk_key:
                return i
        return self.chunks - 1

    def find_key_in_chunk(self, key, chunk):
        with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
            data = f.read().decode("utf8")
            key = f"KEY~{key}~"
            start = data.find(key)
            if start == -1:
                return None

            comma = start + len(key)
            end = data[comma:comma + self.max_len + 1].find("\n")
        return self.dtype(data[comma:comma + end])

    def __contains__(self, key):
        chunk = self.get_chunk_from_key(key)
        value = self.find_key_in_chunk(key, chunk)
        return value is not None

    def __getitem__(self, key):
        chunk = self.get_chunk_from_key(key)
        value = self.find_key_in_chunk(key, chunk)
        if value is None:
            raise KeyError(key)
        return value


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def dump(data, name, dtype=None, is_array=None, chunks=-1, compression=0):
    import numpy as np
    
    if compression == 0:
        compression = zipfile.ZIP_STORED
    elif compression == 1:
        compression = zipfile.ZIP_DEFLATED
    elif compression == 2:
        compression = zipfile.ZIP_BZIP2
    else:
        compression = zipfile.ZIP_LZMA

    if "." in name:
        name, ext = name.rsplit(".", 1)
    else:
        name, ext = name, None

    assert chunks < 999999
    if not os.path.exists(name):
        os.makedirs(name)
    else:
        shutil.rmtree(name)
        os.makedirs(name)

    # compute multiple statistics
    num_entries = len(data)
    assert num_entries > 50
    # infer number of chunks
    if chunks == -1:
        if num_entries <= 1000:
            chunks = 50
        if num_entries <= 10000:
            chunks = 350
        elif num_entries <= 100000:
            chunks = 500
        else:
            chunks = 1000

    # ranges_per_chunk = []
    num_entries_per_chunk = num_entries // chunks
    # sort keys to assign to the corresponding chunk
    sorted_keys = sorted(data.keys())

    # infer datatype if not specified
    if is_array is None:
        first_value = data[sorted_keys[0]]
        if isinstance(first_value, np.ndarray):
            dtype = np.ndarray
            is_array = True
            shape = first_value.shape
        elif isinstance(first_value, str):
            dtype = str
        elif isinstance(first_value, int):
            dtype = int
        elif isinstance(first_value, list):
            dtype = list
        elif isinstance(first_value, dict):
            dtype = dict

    # write chunks
    max_len = 0
    chunk_keys = []
    chunk = ""
    chunk_assign = 1
    chunk_key = sorted_keys[chunk_assign * num_entries_per_chunk]
    assert "~" not in chunk_key and "\n" not in chunk_key
    chunk_keys.append(chunk_key)
    for i, key in tqdm(enumerate(sorted_keys),
                       desc="building dict",
                       total=num_entries):
        assert "~" not in key and "\n" not in key
        # if key does not belong to the current chunk
        if key >= chunk_key and chunk_assign < chunks:
            with open(f"{name}/chunk-{chunk_assign - 1:06d}.txt", "w+") as f:
                f.write(chunk)  # persist chunk to disk
            # update current chunk index
            chunk_assign += 1
            # changed the key to compare
            index_chunk = chunk_assign * num_entries_per_chunk
            if index_chunk < len(sorted_keys):
                chunk_key = sorted_keys[chunk_assign * num_entries_per_chunk]
                assert "~" not in chunk_key
                chunk_keys.append(chunk_key)
            # reset chunk content
            chunk = ""

        # if element is a numpy array
        value = i
        if dtype == str:
            value = data[key]
            assert "~" not in value and "\n" not in value
            str_len = len(str(value))
            if str_len > max_len:
                max_len = str_len
        elif dtype == int:
            value = data[key]
            assert isinstance(value, int)
            str_len = len(str(value))
            if str_len > max_len:
                max_len = str_len
        elif dtype == list or dtype == dict:
            value = json.dumps(data[key])
            str_len = len(str(value))
            if str_len > max_len:
                max_len = str_len

        chunk += f"KEY~{key}~{value}\n"
    if len(chunk) > 0:
        with open(f"{name}/chunk-{chunk_assign - 1:06d}.txt", "w") as f:
            f.write(chunk)  # persist chunk to disk

    with open(f"{name}/config.txt", "w+") as f:
        config = dtype.__name__ + "\n" + str(max_len) + "\n"
        config += "\n".join(chunk_keys)
        f.write(config)

    # zip data
    if ext == "dikt":
        filename = f"{name}.dikt"
        zipf = zipfile.ZipFile(filename, "w", compression)
        zipdir(f"{name}/", zipf)
        zipf.close()
        # shutil.rmtree(f"{name}")


def load(filename):
    return Dikt(filename)
