__version__ = "1.0.2"


class Dikt(object):
    def __init__(
        self,
        filename,
        method="zip",
        cache_values=False,
        cache_chunks=False
    ):
        self.name = filename.split("/")[-1].split(".")[0]
        self.filename = filename
        self.method = method

        if method == "zip":
            from .zipfile2 import ZipFile
            self.zipf = ZipFile(filename)
            with self.zipf.open(self.name + "/config.txt") as f:
                self.parse_config(f)
        elif method == "tar":
            import tarfile
            self.tar = tarfile.open(filename, "r")
            f = self.tar.extractfile(self.name + "/config.txt")
            self.parse_config(f)

        self.cache_values = cache_values
        self.cache_chunks = cache_chunks
        if cache_values:
            self.cached_values = {}
        if cache_chunks:
            self.cached_chunks = {}

    def parse_config(self, f):
        for i, line in enumerate(f):
            line = line.decode("utf8").strip()
            if i == 0:
                dtype = line
                if dtype == "int":
                    self.dtype = int
                elif dtype == "float":
                    self.dtype = float
                elif dtype == "list" or dtype == "dict":
                    from json import loads
                    self.dtype = loads
                    # self.dtype = eval
                elif dtype == "str":
                    self.dtype = lambda x: x
                elif dtype == "ndarray":
                    import numpy as np
                    self.dtype = lambda x: np.array(
                        eval(x), dtype=array_dtype)
            elif i == 1:
                array_dtype = line
            elif i == 2:
                self.max_len = int(line)
            elif i == 3:
                self.num_chunks = int(line)

    def get_chunk_from_key(self, key):
        chunk = hashkey(key, self.num_chunks)
        return chunk

    def get_chunk_data(self, chunk):
        data = None

        # check if already in cache
        if self.cache_chunks:
            if chunk in self.cached_chunks:
                data = self.cached_chunks[chunk]

        # if not, get data
        if data is None:
            with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
                data = f.read().decode("utf8")
        return data

    def find_key_in_data(self, key, data):
        k = f"K~{key}~"
        key_len = len(k)

        # find key, value pair in data
        start = data.find(k)
        if start == -1:
            if self.cache_values:
                self.cached_values[k] = None
            return None

        comma = start + key_len
        end = data[comma:comma + self.max_len + 1].find("\n")
        value = self.dtype(data[comma:comma + end])
        return value

    def find_key_in_chunk(self, key, chunk):
        key = f"K~{key}~"
        key_len = len(key)

        # get data
        data = None
        # check if already in cache
        if self.cache_values:
            if key in self.cached_values:
                return self.cached_values[key]
        if self.cache_chunks:
            if chunk in self.cached_chunks:
                data = self.cached_chunks[chunk]

        # if not, get data
        if data is None:
            if self.method == "zip":
                with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
                    data = f.read().decode("utf8")
            elif self.method == "tar":
                data = self.tar.extractfile(
                    self.name + f"/chunk-{chunk:06d}.txt").read().decode("utf8")
                # print(f)

        # find key, value pair in data
        start = data.find(key)
        if start == -1:
            if self.cache_values:
                self.cached_values[key] = None
            return None

        comma = start + key_len
        end = data[comma:comma + self.max_len + 1].find("\n")
        value = self.dtype(data[comma:comma + end])

        # populate cache
        if self.cache_values:
            self.cached_values[key] = value
        if self.cache_chunks:
            self.cached_chunks[chunk] = data
        return value

    def __contains__(self, key):
        chunk = self.get_chunk_from_key(key)
        value = self.find_key_in_chunk(key, chunk)
        return value is not None

    def __getitem__(self, key):
        if isinstance(key, list):
            return self.get_keys(key)
        else:
            if not isinstance(key, str):
                key = str(key)
            return self.get_key(key)

    def get(self, key, value=None):
        try:
            return self.get_key(key)
        except KeyError:
            return value

    def get_key(self, key):
        key = str(key)
        chunk = self.get_chunk_from_key(key)
        value = self.find_key_in_chunk(key, chunk)
        if value is None:
            raise KeyError(key)
        return value

    def get_keys(self, keys):
        from collections import defaultdict

        results = [None] * len(keys)
        chunk2keys = defaultdict(list)
        key2pos = defaultdict(list)
        for i, key in enumerate(keys):
            chunk = self.get_chunk_from_key(key)
            chunk2keys[chunk].append(key)
            key2pos[key].append(i)

        for chunk, chunk_keys in chunk2keys.items():
            data = self.get_chunk_data(chunk)
            for current_key in chunk_keys:
                value = self.find_key_in_data(current_key, data)
                for index in key2pos[current_key]:
                    results[index] = value
        return results

    def to_dict(self):
        import re
        d = {}
        for chunk in range(self.num_chunks):
            with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
                data = f.read().decode("utf8").splitlines()
                for line in data:
                    match = re.match(r"K\~(.*)\~(.*)", line)
                    d[match.group(1)] = self.dtype(match.group(2))
        return d

    def __iter__(self):
        import re
        for chunk in range(self.num_chunks):
            with self.zipf.open(self.name + f"/chunk-{chunk:06d}.txt") as f:
                data = f.read().decode("utf8").splitlines()
                for line in data:
                    match = re.match(r"K\~(.*)\~(.*)", line)
                    key = match.group(1)
                    value = self.dtype(match.group(2))
                    yield key, value


def zipdir(path, ziph):
    import os
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def dump(
    data,
    filename,
    dtype=None,
    chunks=-1,
    items_per_chunk=None,
    compression=1,
    method="zip",
    verbose=True
):
    from .utils import (
        infer_compression, infer_dtype, clean_folder, infer_chunks,
        remove_tmp_folder, get_path, get_iterator, verify)
    from collections import defaultdict
    from zipfile import ZipFile
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

    if method == "zip":
        zipf = ZipFile(filename, "w", compression)
        zipdir(f"{name}/", zipf)
        zipf.close()
        remove_tmp_folder(name)
    elif method == "tar":
        import tarfile
        import os.path
        with tarfile.open(filename, "w|7z") as tar:
            tar.add(name, arcname=os.path.basename(name))


def load(filename, method="zip", cache_values=False, cache_chunks=False):
    return Dikt(
        filename,
        method=method,
        cache_values=cache_values,
        cache_chunks=cache_chunks)


def hashkey(text, num_chunks):
    if not isinstance(text, str):
        text = str(text)

    hashsum = 0
    text_len = len(text)
    for i, c in enumerate(text, 1):
        ord_c = ord(c)
        hashsum += (text_len + i) ** ord_c + ord_c
    hashsum = hashsum % num_chunks
    return hashsum
