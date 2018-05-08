import stringcase


def dict_to_camel_case(d):
    new = {}
    for k, v in d.iteritems():
        if isinstance(v, dict):
            v = dict_to_camel_case(v)
        new[stringcase.camelcase(k.lower().replace(' ', '_'))] = v
    return new
