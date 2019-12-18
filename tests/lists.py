import dikt

a = {
    f"word_{i}": float(i)
    for i in range(50000)
}
dikt.dump(a, "test.dikt")
