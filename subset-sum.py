from itertools import combinations


def solve(items):
    # for length in range(1, len(items) + 1):
    #     for subset in combinations(items, length):
    #         if abs(sum(subset) - 12) <= 1:
    #             print(subset)
    #             return
    # print('no')

    TARGET = 52
    closer = 10000
    close_set = set()
    for length in range(1, len(items) + 1):
        for subset in combinations(items, length):
            if abs(sum(subset) - TARGET) < closer:
                closer = abs(sum(subset) - TARGET)
                close_set = set(subset)
    print(close_set)


s = {1, 6, 13, 17, 25, 37, 48, 67}
solve(s)
