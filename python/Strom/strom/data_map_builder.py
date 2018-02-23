

class MappingList(list):
    """This class is a list of tuples mapping source data location into dstream keys"""
    def __init__(self, *args, **kwargs):
        logger.debug("Initialize map list")
        self.update(*args, **kwargs)

    def add_mapping(self, source_loc, dstream_key):
        """add a mapping pair to a list of map rules
        param source_loc: the location of the parameter in the raw data. Can be an index or a list of
        dict keys
        type source_loc: int or list of str
        param dstream_key: list of the keys where the raw parameter will be stored in the DStream
        eg ["measures", "location","val"]
         type dstream_key: list of str"""
        self.append((source_loc, dstream_key))

    def load_mappings(self, source_list, dstream_key_list):
        """Add a list of mappings
        param source_list: a list of source locations as used in add_mapping
        type source_list: list
        """
        if len(source_list) != len(dstream_key_list):
            raise ValueError("Mismatched number of sources and dstream keys")
        else:
            for s_loc, dsk in zip(source_list,dstream_key_list):
                self.add_mapping(s_loc, dsk)


def build_mapping(source_list=None, dstream_key_list=None):
    ml = MappingList()
    if source_list is not None:
        ml.load_mappings(source_list, dstream_key_list)

    return ml