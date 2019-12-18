from tqdm import tqdm
from enum import Enum
import zipfile
import json
import os


__version__ = "0.0.5"


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
            self.chunks = len(self.chunk_keys)

        if dtype == "int":
            self.dtype = int
        elif dtype == "float":
            self.dtype = float
        elif dtype == "list" or dtype == "dict":
            self.dtype = json.loads
        elif dtype == "str":
            self.dtype = lambda x: x
        elif dtype == "ndarray":
            import numpy as np
            self.dtype = lambda x: np.array(eval(x), dtype=array_dtype)

    def get_chunk_from_key(self, key):
        for i, chunk_key in enumerate(self.chunk_keys):
            if key < chunk_key:
                return i - 1
        return self.chunks - 1

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
    from .utils import (
        infer_compression, infer_dtype, clean_folder,
        remove_tmp_folder, get_path, infer_chunks,
        get_iterator, get_chunk_keys, verify)
    import numpy as np

    # compression type
    compression = infer_compression(compression)

    # get names
    path, name, ext = get_path(filename)

    # clean zip folder
    clean_folder(name)

    # compute multiple statistics
    num_entries = len(data)

    # infer number of chunks
    chunks = infer_chunks(num_entries, chunks)

    # sort keys to assign to the corresponding chunk
    sorted_keys = sorted(data.keys())

    # infer datatype if not specified
    dtype, array_dtype = infer_dtype(data, sorted_keys)

    # separate keys into chunks
    chunk_keys = get_chunk_keys(sorted_keys, chunks)

    # write chunks
    # ~~~~~~~~~~~~
    chunk = ""
    max_len = 0
    chunk_assign = 0
    for i, key in get_iterator(sorted_keys, num_entries, verbose):
        # verify the key is correctly formed
        verify(key)

        # if key does not belong to the current chunk
        if chunk_assign != len(chunk_keys) - 1 and \
                key >= chunk_keys[chunk_assign + 1]:
            with open(f"{name}/chunk-{chunk_assign:06d}.txt", "w+") as f:
                f.write(chunk)  # persist chunk to disk
            # update current chunk index
            chunk_assign += 1
            # reset chunk content
            chunk = ""

        # if element is a numpy array
        value = i
        if dtype == str:
            value = data[key]
            verify(value)
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
        elif dtype == float:
            value = data[key]
            assert isinstance(value, float)
            str_len = len(str(value))
            if str_len > max_len:
                max_len = str_len

        chunk += f"K~{key}~{value}\n"

    # dump the rest
    if len(chunk) > 0:
        with open(f"{name}/chunk-{chunk_assign:06d}.txt", "w") as f:
            f.write(chunk)  # persist chunk to disk

    with open(f"{name}/config.txt", "w+") as f:
        config = "\n".join([dtype.__name__, array_dtype, str(max_len)]) + "\n"
        config += "\n".join(chunk_keys)
        f.write(config)

    # zip data
    if ext == "dikt":
        if len(path) > 0:
            filename = f"{path}/{name}.dikt"
        else:
            filename = f"{name}.dikt"
        zipf = zipfile.ZipFile(filename, "w", compression)
        zipdir(f"{name}/", zipf)
        zipf.close()
        remove_tmp_folder(name)


def load(filename):
    return Dikt(filename)
