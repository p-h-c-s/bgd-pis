import redis
import json

r = redis.Redis()

path = '../pi2pa/data/amazon-meta.txt'
pathDebug = '../pi2pa/data/debug.txt'

def create_obj(array):
    obj = {}
    obj[array[0][0]] = int(array[0][1].strip())  # Id
    obj[array[1][0]] = array[1][1].strip()  # ASIN

    title = ''
    for til in array[2][1:-2]:
        title += til
    title += array[2][-1]
    obj[array[2][0].strip()] = title  # title

    obj[array[3][0].strip()] = array[3][1].strip()  # group
    obj[array[4][0].strip()] = int(array[4][1].strip())  # salesrank

    obj[array[5][0].strip()] = array[5][1][2:].split()  # similars

    n_categories = int(array[6][1].strip())
    obj[array[6][0].strip()] = []  # categories
    for i in range(0, n_categories):
        obj[array[6][0].strip()].append(array[i+7][0].strip())

    skip = 8+n_categories
    obj['reviews'] = []
    for i in range(skip, (len(array))):
        # print(array[skip+1][0].strip().split()[0])
        obj['reviews'].append({
            'date': array[i][0].strip().split()[0],
            'customer': array[i][1].strip().split()[0],
            'rating': int(array[i][1].strip().split()[2]),
            'votes': int(array[i][1].strip().split()[4]),
            'helpful': int(array[i][1].strip().split()[6])
        })

    return obj

# function to read a file with custom delimiter
def delimited(file, delimiter, bufsize=4096):
    buf = ''
    while True:
        newbuf = file.read(bufsize)
        if not newbuf:
            yield buf
            return
        buf += newbuf
        lines = buf.split(delimiter)
        for line in lines[:-1]:
            yield line
        buf = lines[-1]


def stringToArray(data):
  data = data.split('\n')
  if ('  discontinued product' in data or data[0].startswith('#')):
    return []
  return data

def formatArray(array):
  array = map(lambda obj: obj.split(':', 1), array)
  return list(array)

f = open(path, 'r')

delimited_file = delimited(f, delimiter='\n\n')
for data in delimited_file:
  # stop at last iteration
  if (len(data.replace('\n','')) == 0):
    break
  data = stringToArray(data)
  if len(data) > 0:
    data = formatArray(data)
    obj = create_obj(data)
    r.set(obj['ASIN'], json.dumps(obj))
    # print(json.dumps(obj, indent = 4))