import json
import dikt
import numpy as np
import time


N = 5000000
slice_size = 5000


def timeit(func):
    def wrapper():
        start = time.time()
        func()
        end = time.time()
        print(end - start, "execution time")
        return True
    return wrapper


@timeit
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

    start = time.time()
    res = {}
    keys = [f"key_{i}" for i in np.random.randint(0, N, slice_size)]
    for key in keys:
        res[key] = mapping[key]
    duration = time.time() - start
    print(f"{duration}s to iterate through the dict keys")


@timeit
def get_sliced_items():
    print("\nDikt: Using dikt's slicing method")
    mapping = dikt.load("mapping.dikt")
    keys = [f"key_{i}" for i in np.random.randint(0, N, slice_size)]
    data = mapping[keys]
    for i, item in enumerate(data):
        assert item == int(keys[i].split("_")[1])


@timeit
def get_items():
    print("\nDikt: Iterating over the keys individually")
    mapping = dikt.load("mapping.dikt")
    res = {}
    keys = [f"key_{i}" for i in np.random.randint(0, N, slice_size)]
    for key in keys:
        res[key] = mapping[key]


def check_all_keys_exist():
    print("\nDikt: sanity check, checks that all keys exist")
    mapping = dikt.load("mapping.dikt")
    for i in range(N):
        key = f"key_{i}"
        val = mapping[key]
        if val != i:
            raise AssertionError(
                f"{key} does not match {i} (got {val} instead)")


if __name__ == "__main__":
    create_dikt_mapping()
    get_items_dict()
    # intelligent slicing methods allow slicing
    # to be faster when many keys are provided (ie. > 400)
    get_sliced_items()
    # get_items()
    # check_all_keys_exist()
