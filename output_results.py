import time, sys
import fsop

def print_stats(start_time, total_errors, fsop_ctrs):
    print('')
    print('elapsed time: %9.1f' % (time.time() - start_time))
    print('%9u = total errors' % total_errors)
    print(json.dumps(fsop_ctrs.json_dict(), indent=4))
    sys.stdout.flush()

