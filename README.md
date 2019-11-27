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
mapping = {"key_" + str(i): i for i in range(10000)}
# dump the dictionary to file
dikt.dump(mapping, "mapping.dikt")
# loading object is very fast
mapping = dikt.load("mapping.dikt")
# accessing item directly from the disk
# without loading the whole JSON is ultra fast
print(mapping["key_752"])
```

## Authors

Maixent Chenebaux