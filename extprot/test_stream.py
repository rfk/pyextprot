
from extprot.stream import StringStream

def test():
    s = StringStream()
    s.write_int(34)
    assert StringStream(s.getstring()).read_int() == 34

    s = StringStream()
    s.write_int(128)
    assert StringStream(s.getstring()).read_int() == 128

    s = StringStream()
    s.write_Vint(34)
    s.write_Vint(2053)
    s.write_Vint(314159265)
    assert list(StringStream(s.getstring()).read_values())==[34,2053,314159265]
 

if __name__ == "__main__":
    test()


