import json
import dikt
import numpy as np


N = 5000000
slice_size = 5000


def timeit(func):
    import time

    def wrapper():
        start = time.time()
        func()
        end = time.time()
        print(end - start, "execution time")
        return True
    return wrapper


def create_dikt_mapping():
    mapping = {
        "key_" + str(i): i
        for i in range(N)
    }
    dikt.dump(mapping, "mapping.dikt")
    with open("mapping.json", "w") as f:
        json.dump(mapping, f)


@timeit
def get_item():
    mapping = dikt.load("mapping.dikt")
    res = mapping["key_55"]
    print(res)


@timeit
def get_items_dict():
    print("\nJson: Loading JSON file and populating results")
    with open("mapping.json", "r") as f:
        mapping = json.load(f)

    res = {}
    keys = [f"key_{i}" for i in np.random.randint(0, N, slice_size)]
    for key in keys:
        res[key] = mapping[key]


@timeit
def get_sliced_items():
    print("\nDikt: Using dikt's slicing method")
    mapping = dikt.load("mapping.dikt")
    mapping[[f"key_{i}" for i in np.random.randint(0, N, slice_size)]]


@timeit
def get_items():
    print("\nDikt: Iterating over the keys individually")
    mapping = dikt.load("mapping.dikt")
    res = {}
    keys = [f"key_{i}" for i in np.random.randint(0, N, slice_size)]
    for key in keys:
        res[key] = mapping[key]


if __name__ == "__main__":
    create_dikt_mapping()
    get_items_dict()
    # intelligent slicing methods allow slicing
    # to be faster when many keys are provided (ie. > 400)
    get_sliced_items()
    get_items()
