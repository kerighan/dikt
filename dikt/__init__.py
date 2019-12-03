from tqdm import tqdm
from enum import Enum
import zipfile
import shutil
import json
import os


__version__ = "0.0.2"


class Dikt(object):
    def __init__(self, filename):
        self.zipf = zipfile.ZipFile(filename)
        self.name = filename.split("/")[-1].split(".")[0]
        with self.zipf.open(self.name + "/config.txt") as f:
            data = f.read().decode("utf8")
            lines = data.splitlines()
            dtype = lines[0]
            array_dtype = lines[1]
            self.max_len = int(lines[2])
            self.chunk_keys = tuple(key for key in lines[3:])
            self.chunks = len(self.chunk_keys) + 1

        if dtype == "int":
            self.dtype = int
        elif dtype == "float":
            self.dtype = float
        elif dtype == "list" or dtype == "dict":
            self.dtype = json.loads
        elif dtype == "ndarray":
            import numpy as np
            self.dtype = lambda x: np.array(eval(x), dtype=array_dtype)
            # self.dtype = json.loads

    def get_chunk_from_key(self, key):
        for i, chunk_key in enumerate(self.chunk_keys):
            if key < chunk_key:
                return i
        return self.chunks - 2

    def find_key_in_chunk(self, key, chunk):
        with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
            data = f.read().decode("utf8")
            key = f"K~{key}~"
            start = data.find(key)
            if start == -1:
                return None

            comma = start + len(key)
            end = data[comma:comma + self.max_len + 1].find("\n")
        return self.dtype(data[comma:comma + end])
    
    def find_keys_in_chunk(self, keys, chunk):
        keys = sorted(keys)
        with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
            data = f.read().decode("utf8")
            res = []
            for key in keys:
                key = f"K~{key}~"
                start = data.find(key)
                if start == -1:
                    res.append(None)
                else:
                    comma = start + len(key)
                    end = data[comma:comma + self.max_len + 1].find("\n")
                    res.append(self.dtype(data[comma:comma + end]))
                    data = data[comma + end:]
        return res

    def __contains__(self, key):
        chunk = self.get_chunk_from_key(key)
        value = self.find_key_in_chunk(key, chunk)
        return value is not None

    def __getitem__(self, key):
        if isinstance(key, list):
            return self.get_keys(key)
        else:
            return self.get_key(key)

    def get_key(self, key):
        key = str(key)
        chunk = self.get_chunk_from_key(key)
        value = self.find_key_in_chunk(key, chunk)
        if value is None:
            raise KeyError(key)
        return value
    
    def get_keys(self, keys):
        results = {}
        if len(keys) >= 400:
            from collections import defaultdict
            resp_chunks = defaultdict(list)
            for i, key in enumerate(keys):
                chunk = self.get_chunk_from_key(key)
                resp_chunks[chunk].append(key)

            for chunk, keys in resp_chunks.items():
                res = self.find_keys_in_chunk(keys, chunk)
                for i in range(len(keys)):
                    results[keys[i]] = res[i]
        else:
            for key in keys:
                try:
                    results[key] = self.get_key(key)
                except KeyError:
                    results[key] = None
        return results


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def dump(data, filename, dtype=None, chunks=-1, compression=0, verbose=False):
    import numpy as np

    # compression type
    if compression == 0:
        compression = zipfile.ZIP_STORED
    elif compression == 1:
        compression = zipfile.ZIP_DEFLATED
    elif compression == 2:
        compression = zipfile.ZIP_BZIP2
    else:
        compression = zipfile.ZIP_LZMA

    path, name = filename.rsplit("/", 1)
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
    # infer number of chunks
    assert num_entries >= 1
    if chunks == -1:
        if num_entries <= 100:
            chunks = 1
        elif num_entries <= 1000:
            chunks = 50
        elif num_entries <= 10000:
            chunks = 350
        elif num_entries <= 100000:
            chunks = 500
        else:
            chunks = 1000
    assert num_entries >= chunks

    # ranges_per_chunk = []
    num_entries_per_chunk = num_entries // chunks
    # sort keys to assign to the corresponding chunk
    sorted_keys = sorted(data.keys())

    # infer datatype if not specified
    first_value = data[sorted_keys[0]]
    array_dtype = "none"
    if isinstance(first_value, np.ndarray):
        dtype = np.ndarray
        array_dtype = first_value.dtype.__str__()
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

    if verbose:
        iterator = tqdm(enumerate(sorted_keys),
                        desc="building dict",
                        total=num_entries)
    else:
        iterator = enumerate(sorted_keys)
    for i, key in iterator:
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
            str_len = len(value)
            if str_len > max_len:
                max_len = str_len
        elif dtype == np.ndarray:
            value = json.dumps(list(data[key]))
            str_len = len(value)
            if str_len > max_len:
                max_len = str_len

        chunk += f"K~{key}~{value}\n"
    if len(chunk) > 0:
        with open(f"{name}/chunk-{chunk_assign - 1:06d}.txt", "w") as f:
            f.write(chunk)  # persist chunk to disk

    with open(f"{name}/config.txt", "w+") as f:
        config = "\n".join([dtype.__name__, array_dtype, str(max_len)]) + "\n"
        config += "\n".join(chunk_keys)
        f.write(config)

    # zip data
    if ext == "dikt":
        filename = f"{path}/{name}.dikt"
        zipf = zipfile.ZipFile(filename, "w", compression)
        zipdir(f"{name}/", zipf)
        zipf.close()
        shutil.rmtree(f"{name}")


def load(filename):
    return Dikt(filename)
