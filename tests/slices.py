import json
import dikt
import numpy as np
import pickle
import time


N = 500000
slice_size = 10


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
    # create mapping
    mapping = {
        "key_" + str(i): list(range(i, i + 100))
        for i in range(N)
    }

    # dump using Dikt
    start = time.time()
    dikt.dump(mapping, "mapping.dikt", compression=3)
    duration = time.time() - start
    print(f"[*] saved with Dikt in {duration}")

    # dump using Json
    start = time.time()
    with open("mapping.json", "w") as f:
        json.dump(mapping, f)
    duration = time.time() - start
    print(f"[*] saved with Json in {duration}")

    # dump using Pickle
    start = time.time()
    with open("mapping.p", "wb") as f:
        pickle.dump(mapping, f)
    duration = time.time() - start
    print(f"[*] saved with Pickle in {duration}")


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
def get_items_pickle():
    print("\nPickle: Loading Pickle file and populating results")
    with open("mapping.p", "rb") as f:
        mapping = pickle.load(f)

    start = time.time()
    res = {}
    keys = [f"key_{i}" for i in np.random.randint(0, N, slice_size)]
    for key in keys:
        res[key] = mapping[key]
    duration = time.time() - start
    print(f"{duration}s to iterate through the pickle object keys")


@timeit
def get_items_dikt():
    print("\nDikt: Using dikt's slicing method")
    mapping = dikt.load("mapping.dikt")
    keys = [f"key_{i}" for i in np.random.randint(0, N, slice_size)]
    mapping[keys]

    # uncomment to check everything went ok
    # for i, item in enumerate(data):
    #     assert item == int(keys[i].split("_")[1])


@timeit
def get_items_one_by_one():
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
    # create_dikt_mapping()
    get_items_dict()
    # intelligent slicing methods allow slicing
    # to be faster when many keys are provided (ie. > 400)
    get_items_pickle()
    get_items_dikt()
    # check_all_keys_exist()
