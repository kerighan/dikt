import shutil
import os


def infer_dtype(data, sorted_keys):
    """get correct data type for the first element."""
    import numpy as np

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
    elif isinstance(first_value, float):
        dtype = float
    return dtype, array_dtype


def infer_compression(compression):
    """convert int:`compression` to correct compression level"""
    import zipfile
    if compression == 0:
        compression = zipfile.ZIP_STORED
    elif compression == 1:
        compression = zipfile.ZIP_DEFLATED
    elif compression == 2:
        compression = zipfile.ZIP_BZIP2
    else:
        compression = zipfile.ZIP_LZMA
    return compression


def clean_folder(name):
    if not os.path.exists(name):
        os.makedirs(name)
    else:
        shutil.rmtree(name)
        os.makedirs(name)


def remove_tmp_folder(name):
    shutil.rmtree(f"{name}")


def get_path(filename):
    """corrections by @philippeitis"""
    path = os.path.dirname(filename)
    name = os.path.basename(filename)
    name, ext = os.path.splitext(name)
    return path, name, ext


def infer_chunks(num_entries, chunks):
    """infer the number of chunks by the size of the dictionary."""
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
    if chunks == 0:
        return 1
    return chunks


def get_iterator(sorted_keys, num_entries, verbose):
    """utility function to add a progress bar if verbosity is set to True."""
    if verbose:
        from tqdm import tqdm
        iterator = tqdm(enumerate(sorted_keys),
                        desc="building dict",
                        total=num_entries)
    else:
        iterator = enumerate(sorted_keys)
    return iterator


def get_chunk_keys(seq, num):
    """speed improvements by @philippeitis"""
    avg = len(seq) / float(num)
    out = [None] * num
    last = 0.0
    for i in range(num):
        out[i] = seq[int(last)]
        last += avg

    max_num_entries = len(seq) // num + len(seq) % num
    return out, max_num_entries


def verify(chunk_key):
    assert "~" not in chunk_key and "\n" not in chunk_key
