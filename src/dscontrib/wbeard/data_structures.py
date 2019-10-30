def nd_iter(arr):
    "arr: np.array"
    if len(arr.shape) == 1:
        return iter(arr)
    elif len(arr.shape) == 1:
        return (e for row in arr for e in row)
    raise NotImplementedError
