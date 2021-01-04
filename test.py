import dikt
import random


# generate a dictionary with 1 million entries
N = 1000000
data = {
    # key can be anything you want
    "key_" + str(i): list(range(i, i + 100)) for i in range(N)
}

# persist to dictionary using dikt
dikt.dump(data, "data.dikt")
del data

# load file
data = dikt.Dikt("data.dikt")

# get item without loading the whole file in RAM
print(data["key_125"])

# or get multiple items at once (here 10k)
keys = [f"key_" + str(random.randint(0, N - 1)) for i in range(10000)]
print(data[keys][0])
