#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import pickle
import shutil
import time
# from fs-drift...
import common

notyet = '.notyet'

def write_sync_file(fpath, contents):
    with open(fpath+notyet, 'w') as sgf:
        sgf.write(contents)
        sgf.flush()
        os.fsync(sgf.fileno())  # file should close when you exit with block
        os.rename(fpath+notyet, fpath)

def write_pickle(fpath, obj):
    with open(fpath+notyet, 'wb') as result_file:
        pickle.dump(obj, result_file)
        result_file.flush()
        os.fsync(result_file.fileno())  # or else reader may not see data
        os.rename(fpath+notyet, fpath)

def read_pickle(fpath):
    with open(fpath, 'rb') as result_file:
        return pickle.load(result_file)

def create_top_dirs(prm):
    is_multi_host = (prm.host_set != [])
    sharepath = prm.network_shared_path
    if os.path.exists(sharepath):
        shutil.rmtree(sharepath)
        if is_multi_host:
            # so all remote clients see that directory was recreated
            time.sleep(2.1)
    common.ensure_dir_exists(sharepath)
    if is_multi_host:
        # workaround to force cross-host synchronization
        os.listdir(sharepath)
        time.sleep(1.1)  # lets NFS mount option actimeo=1 take effect
