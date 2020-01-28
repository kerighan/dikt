import dikt
import time

N = 500000
a = {
    "key_" + str(i): i for i in range(N)
}

dikt.dump(a, "a.dikt", verbose=True, compression=1)

start = time.time()
d = dikt.load("a.dikt")
elapsed = time.time() - start
print(f"{elapsed}s for dikt loading")

total = 0
for i in range(N):
    start = time.time()
    data = d["key_" + str(i)]
    total += time.time() - start
    if i != data:
        raise ValueError(f"{i} != {data}")
# elapsed = time.time() - start
print(f"{total}s for dikt access")
