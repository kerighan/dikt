# Dictionary read on disk

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.


### Installing

You can install the method by typing:
```
pip install dikt
```

### Basic usage

```python
import dikt

# use any homogeneous dictionary
mapping = {"key_" + str(i): i for i in range(1000000)}

# dump the dictionary to file
# compression is an integer from 0 to 3, 3 giving the smallest file size
dikt.dump(mapping, "mapping.dikt", compression=1)

# loading object is extremely fast
mapping = dikt.load("mapping.dikt")

# accessing item directly from the disk
# without loading the whole JSON is ultra fast
print(mapping["key_752"])
```

## Authors

Maixent Chenebaux