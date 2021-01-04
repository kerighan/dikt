from collections import defaultdict
import itertools
import mmap


class Dikt:
    def __init__(self, filename):
        self.filename = filename
        self.map_indices()
        self.max_len = len(self.indices) - 1

    def map_indices(self):
        with open(self.filename, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
                for line in iter(m.readline, b""):
                    self.offset = len(line)
                    self.indices = list(
                        itertools.accumulate(int(i)
                                             for i in line.split()))
                    break

    def __getitem__(self, key):
        if isinstance(key, list):
            return self.get_slice(key)
        return self.get(key)

    def get_slice(self, keys):
        hash2keys = defaultdict(list)
        for i, key in enumerate(keys):
            key_hash = get_djb2(key, max_len=self.max_len)
            query = f"~{key}~"
            hash2keys[key_hash].append((i, query))

        res = [None] * len(keys)
        with open(self.filename, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
                for key_hash, items in hash2keys.items():
                    start = self.indices[key_hash] + self.offset
                    end = self.indices[key_hash + 1] + self.offset
                    line = str(m[start:end], encoding="utf8")

                    for (ind, query) in items:
                        index = line.find(query)
                        if index == -1:
                            continue

                        index += len(query)
                        current_line = line[index:]
                        index_end = current_line.find("@")
                        res[ind] = eval(current_line[:index_end])
        return res

    def get(self, key):
        key_hash = get_djb2(key, max_len=self.max_len)
        query = f"~{key}~"

        start = self.indices[key_hash] + self.offset
        end = self.indices[key_hash + 1] + self.offset

        with open(self.filename, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
                line = str(m[start:end], encoding="utf8")
                index = line.find(query)
                if index == -1:
                    raise KeyError

                index += len(query)
                line = line[index:]
                index_end = line.find("@")
                return eval(line[:index_end])


def get_djb2(key, h=747, max_len=10):
    for x in key:
        h = ((h << 5) + h) + ord(x)
    return h % max_len


def dump(obj, filename, factor=.1):
    from tqdm import tqdm

    n_keys = len(obj)
    max_len = int(n_keys * factor)
    data = defaultdict(list)
    max_line_len = 0
    for key, value in obj.items():
        key_hash = get_djb2(key, max_len=max_len)
        value = str(value)

        assert "~" not in value
        assert "~" not in key
        assert "@" not in value
        assert "@" not in key

        key_value_pair = bytes(f"~{key}~{value}@", "utf8")
        data[key_hash].append(key_value_pair)

        if len(key_value_pair) > max_line_len:
            max_line_len = len(key_value_pair)

    indptr = 0
    indices = []
    text = []
    for i in tqdm(range(max_len)):
        line = data.get(i, [])
        if len(line) == 0:
            indices.append(bytes(str(indptr), "utf8"))
            indptr = 0
        else:
            line = b"".join(line)
            text.append(line)
            indices.append(bytes(str(indptr), "utf8"))
            indptr = len(line)
    indices.append(bytes(str(indptr), "utf8"))

    with open(filename, "wb") as f:
        f.write(b" ".join(indices) + b"\n")
        f.write(
            b"".join(text))


def load(filename):
    return Dikt(filename)
