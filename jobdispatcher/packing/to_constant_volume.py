# from builtins import range


def get(lst, ndx):
    return [lst[n] for n in ndx]


def revargsort(lst):
    return sorted(range(len(lst)), key=lambda i: -lst[i])


def argmax(lst):
    return max(range(len(lst)), key=lst.__getitem__)


def to_constant_volume(
    container, v_max, weight_pos=None, key=None, lower_bound=None, upper_bound=None,
):
    """
    Distributes a list of weights, a dictionary of weights or a list of tuples containing weights
    to a minimal number of bins that have a fixed volume.
    Parameters
    ==========
   container : iterable
        list containing weights,
        OR dictionary where each (key,value)-pair carries the weight as value,
        OR list of tuples where one entry in the tuple is the weight. The position of
        this weight has to be given in optional variable weight_pos
    v_max : int or float
        Fixed bin volume
    weight_pos : int, default = None
        if container is a list of tuples, this integer number gives the position
        of the weight in a tuple
    key : function, default = None
        ifcontainer is a list, this key functions grabs the weight for an item
    lower_bound : float, default = None
        weights under this bound are not considered
    upper_bound : float, default = None
        weights exceeding this bound are not considered
    Returns
    =======
    bins : list
        A list. Each entry is a list of items or
        a dict of items, depending on the type of ``container``.
    """

    isdict = isinstance(container, dict)

    if not hasattr(container, "__len__"):
        raise TypeError("container must be iterable")

    if not isdict and hasattr(container[0], "__len__"):
        if weight_pos is not None:
            key = lambda x: x[weight_pos]
        if key is None:
            raise ValueError("Must provide weight_pos or key for tuple list")

    if not isdict and key:
        # new_dict = {i: val for i, val in enumerate(container)}
        new_dict = dict(enumerate(container))
        container = {i: key(val) for i, val in enumerate(container)}
        isdict = True
        is_tuple_list = True
    else:
        is_tuple_list = False

    if isdict:

        # get keys and values (weights)
        keys_vals = container.items()
        keys = [k for k, v in keys_vals]
        vals = [v for k, v in keys_vals]

        # sort weights decreasingly
        ndcs = revargsort(vals)

        weights = get(vals, ndcs)
        keys = get(keys, ndcs)

        bins = [{}]
    else:
        weights = sorted(container, key=lambda x: -x)
        bins = [[]]

    # find the valid indices
    if (
        lower_bound is not None
        and upper_bound is not None
        and lower_bound < upper_bound
    ):
        valid_ndcs = filter(
            lambda i: lower_bound < weights[i] < upper_bound, range(len(weights))
        )
    elif lower_bound is not None:
        valid_ndcs = filter(lambda i: lower_bound < weights[i], range(len(weights)))
    elif upper_bound is not None:
        valid_ndcs = filter(lambda i: weights[i] < upper_bound, range(len(weights)))
    elif lower_bound is None and upper_bound is None:
        valid_ndcs = range(len(weights))
    elif lower_bound >= upper_bound:
        raise Exception("lower_bound is greater or equal to upper_bound")

    valid_ndcs = list(valid_ndcs)

    weights = get(weights, valid_ndcs)

    if isdict:
        keys = get(keys, valid_ndcs)

    # the total volume is the sum of all weights
    # V_total = sum(weights)

    # prepare array containing the current weight of the bins
    weight_sum = [0.0]

    # iterate through the weight list, starting with heaviest
    for item, weight in enumerate(weights):

        if isdict:
            key = keys[item]

        # find candidate bins where the weight might fit

        # pylint: disable=cell-var-from-loop
        candidate_bins = list(
            filter(lambda i: weight_sum[i] + weight <= v_max, range(len(weight_sum)))
        )
        # pylint: enable=cell-var-from-loop

        # if there are candidates where it fits
        if len(candidate_bins) > 0:

            # find the fullest bin where this item fits and assign it
            candidate_index = argmax(get(weight_sum, candidate_bins))
            selected_bin = candidate_bins[candidate_index]

        # if this weight doesn't fit in any existent bin
        elif item > 0:
            # note! if this is the very first item then there is already an
            # empty bin open so we don't need to open another one.

            # open a new bin
            selected_bin = len(weight_sum)
            weight_sum.append(0.0)
            if isdict:
                bins.append({})
            else:
                bins.append([])

        # if we are at the very first item, use the empty bin already open
        else:
            selected_bin = 0

        # put it in
        if isdict:
            bins[selected_bin][key] = weight
        else:
            bins[selected_bin].append(weight)

        # increase weight sum of the bin and continue with
        # next item
        weight_sum[selected_bin] += weight

    if not is_tuple_list:
        return bins

    new_bins = []

    for selected_bin, _ in enumerate(bins):
        # for selected_bin in range(len(bins)):
        new_bins.append([])
        for _key in bins[selected_bin]:
            new_bins[selected_bin].append(new_dict[_key])
    return new_bins
