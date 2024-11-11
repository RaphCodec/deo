import os
from datetime import datetime
from random import randint


def parse_ranges(ranges_str):
    """
    Parses a string of ranges and returns a list of integers.
    Args:
        ranges_str (str): A string containing ranges separated by commas.
    Returns:
        list: A list of integers representing the parsed ranges.
    Example:
        >>> parse_ranges("1-3,5,7-9")
        [1, 2, 3, 5, 7, 8, 9]
    """
    result = []
    ranges = ranges_str.split(",")

    for r in ranges:
        if "-" in r:
            start, end = map(int, r.split("-"))
            result.extend(range(start, end + 1))
        else:
            result.append(int(r))

    return result


def check_file(file_path):
    if os.path.exists(file_path):
        modified_time = os.path.getmtime(file_path)
        modified_date = datetime.fromtimestamp(modified_time).date()
        current_date = datetime.now().date()
        if modified_date != current_date:
            return False
    else:
        return False
    return True


def rand_int_list(max_num, values_returned):
    if max_num < 0 or values_returned < 1:
        raise ValueError(
            "max_num should be non-negative and values_returned should be at least 1"
        )

    return [randint(0, max_num) for _ in range(values_returned)]


def unpack_list(lst: list, peren=True):
    if not peren:
        return ", ".join(map(str, lst))
    else:
        return f"({', '.join(map(str, lst))})"
