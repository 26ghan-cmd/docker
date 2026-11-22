import os
def load_table(path):
    """Return a sorted list of row tuples (bytes) from a csv file, or a parquet dir / csv beside it."""
    if os.path.isdir(path):
        from pq import read_dir
        return sorted(read_dir(path))
    if not os.path.exists(path) and os.path.exists(path + '.csv'):
        path += '.csv'
    lines = open(path, 'rb').read().split(b'\n')
    if lines and lines[-1] == b'': lines.pop()
    hdr = lines[0].split(b',')
    order = sorted(range(len(hdr)), key=lambda i: hdr[i])
    return sorted(tuple(r.split(b',')[i] for i in order) for r in lines[1:])
