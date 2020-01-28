from zipfile import ZipFile


__version__ = "1.0.0"


class Dikt(object):
    def __init__(self, filename, cache=False):
        self.zipf = ZipFile(filename)
        self.name = filename.split("/")[-1].split(".")[0]
        with self.zipf.open(self.name + "/config.txt") as f:
            data = f.read().decode("utf8")
            lines = data.splitlines()
            dtype = lines[0]
            array_dtype = lines[1]
            self.max_len = int(lines[2])
            self.num_chunks = int(lines[3])

        if dtype == "int":
            self.dtype = int
        elif dtype == "float":
            self.dtype = float
        elif dtype == "list" or dtype == "dict":
            from json import loads
            self.dtype = loads
        elif dtype == "str":
            self.dtype = lambda x: x
        elif dtype == "ndarray":
            import numpy as np
            self.dtype = lambda x: np.array(eval(x), dtype=array_dtype)

        self.cache = cache
        if cache:
            self.cache_chunks = {}
    
    def extract_zip(self, input_zip):
        input_zip=ZipFile(input_zip)
        return {name: input_zip.read(name) for name in input_zip.namelist()}

    def get_chunk_from_key(self, key):
        chunk = hashkey(key, self.num_chunks)
        return chunk

    def find_key_in_chunk(self, key, chunk):
        if self.cache:
            if chunk in self.cache_chunks:
                data = self.cache_chunks[chunk]
            else:
                with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
                    data = f.read().decode("utf8")
                self.cache_chunks[chunk] = data
        else:
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
                    data = data[start:]
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
        if len(keys) >= 400:
            from collections import defaultdict
            results = {}
            resp_chunks = defaultdict(list)
            for i, key in enumerate(keys):
                chunk = self.get_chunk_from_key(key)
                resp_chunks[chunk].append(key)

            for chunk, chunk_keys in resp_chunks.items():
                sorted_keys = sorted(chunk_keys)
                res = self.find_keys_in_chunk(sorted_keys, chunk)
                for i in range(len(sorted_keys)):
                    results[sorted_keys[i]] = res[i]
            return [results[keys[i]] for i in range(len(keys))]
        else:
            results = [None] * len(keys)
            for i, key in enumerate(keys):
                try:
                    results[i] = self.get_key(key)
                except KeyError:
                    pass
            return results


def zipdir(path, ziph):
    import os
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def dump(data, filename, dtype=None, chunks=-1, items_per_chunk=None, compression=0, verbose=True):
    from .utils import (
        infer_compression, infer_dtype, clean_folder, infer_chunks,
        remove_tmp_folder, get_path, get_iterator, verify)
    from collections import defaultdict
    import numpy as np
    import json
    import os

    # compression type
    compression = infer_compression(compression)

    # get names
    path, name, ext = get_path(filename)

    # clean zip folder
    clean_folder(name)

    # compute multiple statistics
    num_entries = len(data)
    if items_per_chunk is not None:
        num_chunks = num_entries // items_per_chunk
    else:
        num_chunks = infer_chunks(num_entries, chunks)

    # map each key to the right chunk using the hash function
    hash2chunk = defaultdict(list)
    for key in data.keys():
        key_bin = hashkey(key, num_chunks)
        hash2chunk[key_bin].append(key)

    # write files
    dtype, array_dtype = infer_dtype(data[key])        

    # write chunks
    # ~~~~~~~~~~~~
    max_len = 0
    for i in get_iterator(num_chunks, verbose):
        chunks = [None] * len(hash2chunk[i])
        chunk_filename = os.path.join(name, f"chunk-{i:06d}.txt")
        for idx, key in enumerate(hash2chunk[i]):
            verify(key)

            # get string value
            if dtype == str:
                value = data[key]
                verify(value)
            elif dtype == int:
                value = data[key]
                assert isinstance(value, int)
            elif dtype == list or dtype == dict:
                value = json.dumps(data[key])
            elif dtype == np.ndarray:
                value = json.dumps(list(data[key]))
            elif dtype == float:
                value = data[key]
                assert isinstance(value, float)

            value = str(value)
            str_len = len(value)
            if str_len > max_len:
                max_len = str_len
            
            chunks[idx] = f"K~{key}~{value}\n"

        # temporary save chunk to disk
        with open(chunk_filename, "w") as f:
            f.write("".join(chunks))  # persist chunk to disk

    # put config inside folder
    with open(f"{name}/config.txt", "w+") as f:
        config = "\n".join([
            dtype.__name__,
            array_dtype,
            str(max_len),
            str(num_chunks)])
        f.write(config)

    # zip data
    if len(path) > 0:
        filename = f"{path}/{name}{ext}"
    else:
        filename = f"{name}{ext}"
    zipf = ZipFile(filename, "w", compression)
    zipdir(f"{name}/", zipf)
    zipf.close()
    remove_tmp_folder(name)


def load(filename, cache=False):
    return Dikt(filename, cache=False)


def hashkey(text, num_chunks):
    hashsum = 0
    text_len = len(text)
    for i, c in enumerate(text, 1):
        ord_c = ord(c)
        hashsum += (text_len + i) ** ord_c + ord_c
    hashsum = hashsum % num_chunks
    return hashsum
