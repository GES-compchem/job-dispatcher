import pytest
from numpy.testing import assert_array_almost_equal
from multiprocessing import cpu_count
from jobdispatcher.index_based_loops import pFor, Static, Dynamic


# Simple function to be used to test calls without arguments
def function_without_args(n: int) -> int:
    return n + 1

# Simple function to be used to test calls with arguments
def function_with_args(n: int, value: float) -> float:
    return n * value

# TESTS FOR THE Static SCHEDULER CLASS
# ------------------------------------------------------------------------------------------

# Test the Static class constructor
def test_Static___init__():

    try:
        obj = Static(2)
    except:
        assert False, "Unexpected exception raised on Static class construction"
    
    assert obj.cores == 2

    try:
        obj = Static(-1)
    except:
        assert False, "Unexpected exception raised on Static class construction"
    
    assert obj.cores == cpu_count()

# Test the get_chuncks function of the Static class with exact job/core ratio
def test_Static_get_chuncks_exact():

    obj = Static(2)
    result = obj.get_chunks(10)

    assert result == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

# Test the get_chuncks function of the Static class with reminder
def test_Static_get_chuncks_reminder():

    obj = Static(2)
    result = obj.get_chunks(11)

    assert result == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9, 10]]

# Test the get_chuncks function of the Static class with less jobs than cores
def test_Static_get_chuncks_less():

    obj = Static(2)

    try:
        result = obj.get_chunks(1)
    except RuntimeError:
        assert True
    else:
        assert False, "A RuntimeError exception was expected"

# TESTS FOR THE Dynamic SCHEDULER CLASS
# ------------------------------------------------------------------------------------------

# Test the Static class constructor
def test_Dynamic___init__():

    try:
        obj = Dynamic(2, 2)
    except:
        assert False, "Unexpected exception raised on Dynamic class construction"
    
    assert obj.cores == 2

    try:
        obj = Dynamic(-1, 2)
    except:
        assert False, "Unexpected exception raised on Dynamic class construction"
    
    assert obj.cores == cpu_count()

# Test the get_chuncks function of the Dynamic class with exact job/core ratio
def test_Dynamic_get_chuncks_exact():

    obj = Dynamic(2, 2)
    result = obj.get_chunks(10)

    assert result == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]

# Test the get_chuncks function of the Dynamic class with reminder
def test_Dynamic_get_chuncks_reminder():

    obj = Dynamic(2, 3)
    result = obj.get_chunks(10)

    assert result == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

# Test the get_chuncks function of the Dynamic class with less jobs than cores
def test_Dynamic_get_chuncks_less():

    obj = Dynamic(2, 2)
    result = obj.get_chunks(2)

    assert result == [[0, 1]]


# TEST OF THE pFor CLASS
# ------------------------------------------------------------------------------------------

# Test the pFor class constructor
def test_pFor___init__():

    try:
        _ = pFor(Static(2))
    except:
        assert (
            False
        ), "Unexpected exception raised during pFor class construction with Static scheduler"

    try:
        _ = pFor(Dynamic(2, 10))
    except:
        assert (
            False
        ), "Unexpected exception raised during pFor class construction with Dynamic scheduler"


# Test the pFor call of a function without arguments using the Static scheduler
def test_pFor___call___Static_without_args():

    results = pFor(Static(2))(function_without_args, 0, 10, 2)
    assert results == [1, 3, 5, 7, 9]


# Test the pFor call of a function with arguments using the Static scheduler
def test_pFor___call___Static_with_args():

    results = pFor(Static(2))(function_with_args, 0, 10, 2, [1.5])
    assert_array_almost_equal(results, [0, 3, 6, 9, 12], decimal=6)


# Test the pFor call of a function without arguments using the Dynamic scheduler
def test_pFor___call___Dynamic_without_args():

    results = pFor(Dynamic(2, 3))(function_without_args, 0, 10, 2)
    assert results == [1, 3, 5, 7, 9]


# Test the pFor call of a function with arguments using the Dynamic scheduler
def test_pFor___call___Dynamic_with_args():

    results = pFor(Dynamic(2, 3))(function_with_args, 0, 10, 2, [1.5])
    assert_array_almost_equal(results, [0, 3, 6, 9, 12], decimal=6)
