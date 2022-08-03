# for backwards compatibility with earlier python versions

unit_test_module = None
def get_unit_test_module():
    try:
        import unittest
        unit_test_module = unittest
    except ImportError:
        import unittest2
        unit_test_module = unittest2
    return unit_test_module
