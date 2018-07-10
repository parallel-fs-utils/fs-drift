# list2csv.py - conversion between lists and a .csv (comma-separated value) file record format


def list2csv(list_in):
    if len(list_in) == 0:
        return ''
    record = str(list_in[0])
    for element in list_in[1:]:
        record += ',' + str(element)
    return record


def csv2list(csv_record):
    list_out = []
    for t in csv_record.strip().split(','):
        list_out.append(t.strip())
    return list_out
