from csv_dataflow.decompose import decompose

@decompose
def f(l: list[int]) -> list[int]:
    r: list[int] = []

    for v in l:
        r.append(v)

    return r
