from databird_drivers.climate.earthdata import parse_slice


def test_parse_slice():
    try:
        assert parse_slice("")
        assert False, "It should raise TypeError"
    except TypeError:
        pass
    assert parse_slice("2") == slice(2)
    assert parse_slice("2:3") == slice(2, 3)
    assert parse_slice(":3") == slice(None, 3)
    assert parse_slice(":") == slice(None, None)
    assert parse_slice("2:") == slice(2, None)
    assert parse_slice("2:3:4") == slice(2, 4, 3)
    assert parse_slice(":3:4") == slice(None, 4, 3)
    assert parse_slice("2::4") == slice(2, 4, None)
    assert parse_slice("2:3:") == slice(2, None, 3)
    assert parse_slice("::4") == slice(None, 4, None)
    assert parse_slice("2::") == slice(2, None, None)
    assert parse_slice("::") == slice(None, None, None)
    assert parse_slice("-12:-13:-14") == slice(-12, -14, -13)
    assert parse_slice("2:3:-4") == slice(2, -4, 3)
    try:
        parse_slice("1:2:3:4")
        assert False, "It should raise TypeError"
    except TypeError:
        pass
