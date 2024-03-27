from itertools import combinations


def maximize_constrained_partial_product(values, bound):
    def product(inds):
        p = 1
        for i in inds:
            p *= values[i]
        return p

    allprod = product(range(len(values)))
    if allprod <= bound:
        return ()

    s = sorted(
        list(
            (c, p)
            for r in range(1, len(values) + 1)
            for c in combinations(range(len(values)), r)
            if bound * (p := product(c)) >= allprod
        ),
        key=lambda pc: pc[1],
    )
    return s[0][0]


assert maximize_constrained_partial_product((1,), 3) == ()
assert maximize_constrained_partial_product((1, 2), 3) == ()
assert maximize_constrained_partial_product((1, 2, 3), 3) == (1,)
assert maximize_constrained_partial_product((1, 10, 5, 6), 60) == (2,)
assert maximize_constrained_partial_product((2, 9, 5, 7), 60) == (
    0,
    3,
)
