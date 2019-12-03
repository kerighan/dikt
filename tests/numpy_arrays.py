import json
import dikt
import numpy as np
from slices import timeit
from numpy.lib.format import open_memmap


N = 100000
dim = 200
X = np.random.random((N, dim))
Y = np.zeros(
    dim,
    dtype=[(f"key_{i}", np.double) for i in range(N)]
)


def create_dikt_mapping():
    from tqdm import tqdm

    mapping = {}
    for i in tqdm(range(N)):
        key = "key_" + str(i)
        value = (np.arange(200) + i).astype(np.double)
        Y[key] = value
        mapping[key] = value

    dikt.dump(mapping, "mapping.dikt")
    fp = np.memmap("mapping.dat",
                   dtype='float64',
                   mode='w+',
                   shape=Y.shape)
    for i in tqdm(range(N)):
        key = "key_" + str(i)
        fp[key] = Y[key]
    # np.save("mapping.npy", Y)


@timeit
def get_item():
    print("\nDikt can manage numpy array, "
          "though not as good as numpy's Memmap")
    mapping = dikt.load("mapping.dikt")
    print(mapping["key_554"][:10])


@timeit
def get_item_from_memmap():
    print("\nNumpy's Memmap is faster, "
          "but to be fair there is no string mapping here")
    fp = np.memmap("mapping.dat",
                   dtype="float64",
                   mode="r",
                   shape=(N, dim))
    print(fp[554][:10])


@timeit
def get_sliced_items():
    mapping = dikt.load("mapping.dikt")
    mapping[[f"key_{i}" for i in np.random.randint(0, N, 399)]]
    print("\nVector-slicing: Dikt is not in its comfort zone here, "
          "but manages to be still decent")


# @timeit
# def get_sliced_items_from_memmap():
#     fp = np.memmap("mapping.dat",
#                    dtype="float64",
#                    mode="r",
#                    shape=(N, dim))
#     indices = np.random.randint(0, N, 399)
#     fp[indices]
#     print("\nNumpy's Memmap is incredibly fast when slicing, "
#           "there is no competition here.")


@timeit
def get_sliced_items_from_memmap():
    Y = open_memmap(
        "mapping.npy",
        mode="r")
    indices = np.random.randint(0, N, 399)
    print(Y["key_55"])
    

if __name__ == "__main__":
    create_dikt_mapping()
    # get_item()
    # get_item_from_memmap()
    # get_sliced_items()
    # get_sliced_items_from_memmap()
    # print(Y["key_55"])
    # get_sliced_items_from_memmap()
