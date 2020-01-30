import shutil
import os


def infer_dtype(value):
    """get correct data type for the first element."""
    import numpy as np

    array_dtype = "none"
    if isinstance(value, np.ndarray):
        dtype = np.ndarray
        array_dtype = value.dtype.__str__()
    elif isinstance(value, str):
        dtype = str
    elif isinstance(value, int):
        dtype = int
    elif isinstance(value, list):
        dtype = list
    elif isinstance(value, dict):
        dtype = dict
    elif isinstance(value, float):
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


def get_iterator(num_chunks, verbose):
    """utility function to add a progress bar if verbosity is set to True."""
    if verbose:
        from tqdm import tqdm
        iterator = tqdm(range(num_chunks),
                        desc="building dict")
    else:
        iterator = range(num_chunks)
    return iterator


def verify(chunk_key):
    assert isinstance(chunk_key, str)
    assert "~" not in chunk_key and "\n" not in chunk_key


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
