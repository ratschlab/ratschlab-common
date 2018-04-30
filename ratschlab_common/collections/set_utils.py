import doctest  # NOQA


def are_pairwise_disjoint(it, raise_error=False):
    """
    Determines whether a collection of elements are pairwise disjoint.

    :param it: iterable of elements
    :param raise_error: if a (descriptive) error shall be raised in case the
    elements iterables are not disjoint
    :return: true if all elements are pairwise disjoint

    >>> are_pairwise_disjoint([[1,2,3], [4,5,6], [7,8]])
    True
    >>> are_pairwise_disjoint([[1,2,3], [1,5]])
    False

    """
    all_elements = set()
    for (i, l) in enumerate(it):
        s = set(l)
        if all_elements.isdisjoint(s):
            all_elements = all_elements.union(s)
        else:
            if raise_error:
                raise ValueError('Set at index {} is not disjoint with'
                                 'previous sets. Common entries are {}'.
                                 format(i, all_elements.intersection(s)))
            return False

    return True
