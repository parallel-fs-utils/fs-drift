# define class for counters so we can 
# easily export them or convert them to JSON

class FSOPCounters:

    def __init__(self):
        # operation counters, incremented by op function below
        self.have_created = 0
        self.have_deleted = 0
        self.have_softlinked = 0
        self.have_hardlinked = 0
        self.have_appended = 0
        self.have_randomly_written = 0
        self.have_read = 0
        self.have_randomly_read = 0
        self.have_renamed = 0
        self.have_truncated = 0
        self.have_remounted = 0
        
        # throughput counters
        self.read_requests = 0
        self.read_bytes = 0
        self.randread_requests = 0
        self.randread_bytes = 0
        self.write_requests = 0
        self.write_bytes = 0
        self.randwrite_requests = 0
        self.randwrite_bytes = 0
        self.fsyncs = 0
        self.fdatasyncs = 0
        self.dirs_created = 0
        
        # error counters
        self.e_already_exists = 0
        self.e_file_not_found = 0
        self.e_no_dir_space = 0
        self.e_no_inode_space = 0
        self.e_no_space = 0
        self.e_not_mounted = 0
        self.e_could_not_mount = 0


    # used to aggregate per-thread counters 
    # into per-host and per-cluster counters

    def add_to(self, total):

        # operation counters, incremented by op function below
        total.have_created          += self.have_created
        total.have_deleted          += self.have_deleted
        total.have_softlinked       += self.have_softlinked
        total.have_hardlinked       += self.have_hardlinked
        total.have_appended         += self.have_appended
        total.have_randomly_written += self.have_randomly_written
        total.have_read             += self.have_read
        total.have_randomly_read    += self.have_randomly_read
        total.have_renamed          += self.have_renamed
        total.have_truncated        += self.have_truncated
        total.have_remounted        += self.have_remounted
        
        # throughput counters
        total.read_requests         += self.read_requests
        total.read_bytes            += self.read_bytes
        total.randread_requests     += self.randread_requests
        total.randread_bytes        += self.randread_bytes
        total.write_requests        += self.write_requests
        total.write_bytes           += self.write_bytes
        total.randwrite_requests    += self.randwrite_requests
        total.randwrite_bytes       += self.randwrite_bytes
        total.fsyncs                += self.fsyncs
        total.fdatasyncs            += self.fdatasyncs
        total.dirs_created          += self.dirs_created
        
        # error counters
        total.e_already_exists      += self.e_already_exists
        total.e_file_not_found      += self.e_file_not_found
        total.e_no_dir_space        += self.e_no_dir_space
        total.e_no_inode_space      += self.e_no_inode_space
        total.e_no_space            += self.e_no_space
        total.e_not_mounted         += self.e_not_mounted
        total.e_could_not_mount     += self.e_could_not_mount

    # next 3 functions summarize activity

    def total_files(self):
        return self.have_created + self.have_deleted + \
            self.have_softlinked + self.have_hardlinked + self.have_truncated + \
            self.have_appended + self.have_randomly_written + \
            self.have_read + self.have_randomly_read 

    def total_ios(self):
        return self.read_requests + self.randread_requests + self.write_requests + self.randwrite_requests

    def total_bytes(self):
        return self.read_bytes + self.read_bytes + self.randwrite_bytes + self.write_bytes

    # first convert counters to a key-value tuple list,
    # we can get them into any other form easily from that

    def kvtuplelist(self):
        return [
            ('created', self.have_created),
            ('deleted', self.have_deleted),
            ('softlinked', self.have_softlinked),
            ('hardlinked', self.have_hardlinked),
            ('appended', self.have_appended),
            ('randomly_written', self.have_randomly_written),
            ('sequentially_read', self.have_read),
            ('randomly_read', self.have_randomly_read),
            ('renamed', self.have_renamed),
            ('truncated', self.have_truncated),
            ('remounted', self.have_remounted),
            ('read_requests', self.read_requests),
            ('read_bytes', self.read_bytes),
            ('randread_requests', self.randread_requests),
            ('randread_bytes', self.randread_bytes),
            ('write_requests', self.write_requests),
            ('write_bytes', self.write_bytes),
            ('randwrite_requests', self.randwrite_requests),
            ('randwrite_bytes', self.randwrite_bytes),
            ('fsyncs', self.fsyncs),
            ('fdatasyncs', self.fdatasyncs),
            ('dirs_created', self.dirs_created),
            ('e_already_exists', self.e_already_exists),
            ('e_file_not_found', self.e_file_not_found),
            ('e_no_dir_space', self.e_no_dir_space),
            ('e_no_inode_space', self.e_no_inode_space),
            ('e_no_space', self.e_no_space),
            ('e_not_mounted', self.e_not_mounted),
            ('e_could_not_mount', self.e_could_not_mount)
            ]

    def __str__(self):
        field_list = [ '%20s = %d' % f for f in self.kvtuplelist() ]
        return '\n'.join(field_list)

    def json_dict(self):
        d = {}
        for (k, v) in self.kvtuplelist():
            d[k] = v
        return d

