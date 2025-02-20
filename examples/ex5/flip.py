from csv_dataflow.decompose import decompose

class A: ...
class B: ...

@decompose
def flip(b: A | B) -> A | B:
    match b:
        case A():
            return B()
        case B():
            return A()
