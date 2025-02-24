from csv_dataflow.decompose import decompose

class A: ...
class B: ...

@decompose
def flip(x: A | B) -> A | B:
    y: B | A
    match x:
        case A():
            y = B()
        case B():
            y = A()
    return y
