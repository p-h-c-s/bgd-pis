def myreadlines(f, newline):
  buf = ""
  while True:
    while newline in buf:
      pos = buf.index(newline)
      yield buf[:pos]
      buf = buf[pos + len(newline):]
    chunk = f.read(4096)
    if not chunk:
      yield buf
      break
    buf += chunk

with open('data/amazon-meta.txt', 'r') as f:
  for l in myreadlines(f, "\n\n"):
    l = str(l)
    if (not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total"))):
      l = l.splitlines()
      l = [elem.split(':') for elem in l]
      l = [tuple([x.strip() for x in elem]) for elem in l]
      l = (l[3][1], (l[1][1], l[4][1]))
      print('\n')
      print(l)