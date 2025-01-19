# tar_filter.py version 0.1.0 2025
#
# Copyright 2025 iRacly <iracly@hotmail.com>
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# TODO(next): arguments parsing
import copy
import io
import json
import os
import tarfile  # https://docs.python.org/3/library/tarfile.html
import time


def dump_TarInfo(m: tarfile.TarInfo):
    print(f'name        : {m.name}')
    print(f'size        : {m.size}')
    print(f'mtime       : {m.mtime}')
    print(f'mode        : {m.mode}')
    print(f'type        : {m.type}')
    print(f'linkname    : {m.linkname}')
    print(f'uid         : {m.uid}')
    print(f'gid         : {m.gid}')
    print(f'uname       : {m.uname}')
    print(f'gname       : {m.gname}')
    print(f'pax_headers : {str(m.pax_headers)}')
    print(f'isfile : {m.isfile()}')  # True for a regular file.
    print(f'isreg  : {m.isreg()}')   # Same as isfile().
    print(f'isdir  : {m.isdir()}')   # True if it is a directory.
    print(f'issym  : {m.issym()}')   # True if it is a symbolic link.
    print(f'islnk  : {m.islnk()}')   # True if it is a hard link.
    print(f'ischr  : {m.ischr()}')   # True if it is a character device.
    print(f'isblk  : {m.isblk()}')   # True if it is a block device.
    print(f'isfifo : {m.isfifo()}')  # True if it is a FIFO.
    # m.isdev() -- True if it is one of character device, block device or FIFO:
    print(f'isdev  : {m.isdev()}')
    return


def file_stem(some_path: str):
    bn = os.path.basename(some_path)
    return bn[:bn.find('.')]


def do_keep_tar_item(m: tarfile.TarInfo) -> (bool, tarfile.TarInfo):
    sepos = m.name.find('/')
    if -1 == sepos:
        return False, None
    name = m.name[sepos + 1:]
    useless_folder = False  # todo(filtering) detect useless folders
    if useless_folder:
        return False, None

    # todo(filtering) detect useless files
    useless_file = name.endswith('.bak') or name.endswith('.orig')
    if useless_file:
        return False, None
    n = copy.copy(m)  # starting 3.8.17: n = m.replace(name=nname)
    n.name = name
    return True, n


def main(src_tar: str, dst_tar: str):
    print(f'converting:     {src_tar}')
    print(f'save result to: {dst_tar}')
    t1 = time.time()
    src = tarfile.open(name=src_tar, mode='r', bufsize=515 * 1024 * 1024)
    t2 = time.time()
    print('-- open SOURCE in %ds' % int(t2 - t1))
    dst = tarfile.open(name=dst_tar, mode='w', bufsize=1024000)
    print('-- open DESTINATION in %ds' % int(time.time() - t2))
    src_total_items = 0
    src_files_count = 0
    src_folders_count = 0
    src_links_count = 0
    src_others_count = 0
    src_total_bytes = 0
    src_ignored_items = 0
    suppressed = set()
    symbolic_links = dict()
    hard_links = dict()
    dst_files_count = 0
    dst_folders_count = 0
    dst_others_count = 0
    dst_links_count = 0
    dst_total_items = 0
    dst_total_bytes = 0
    while (m := src.next()) is not None:
        src_total_items += 1
        src_files_count += 1 if m.isfile() else 0
        src_folders_count += 1 if m.isdir() else 0
        src_links_count += 1 if m.issym() or m.islnk() else 0
        src_others_count += 1 if m.isdev() else 0
        src_total_bytes += m.size

        keep, n = do_keep_tar_item(m)
        if not keep:
            src_ignored_items += 1
            suppressed.add(m.name)
            continue

        print(f'#{src_total_items} {n.name:32} {m.size}B')
        try:
            f = src.extractfile(m) if m.isfile() else None
            if m.islnk():
                sepos = m.linkname.find('/')
                n.linkname = m.linkname[sepos + 1:]
                hard_links[m.name] = m.linkname
            if m.issym():
                symbolic_links[m.name] = m.linkname
        except Exception as e:
            print(f'something bad happens with {n.name}')
            print(e)
            dump_TarInfo(m)
            continue
        # starting 3.8.17: n = m.replace(name=n.name)
        dst_total_items += 1
        dst_files_count += 1 if n.isfile() else 0
        dst_folders_count += 1 if n.isdir() else 0
        dst_links_count += 1 if n.issym() or n.islnk() else 0
        dst_others_count += 1 if n.isdev() else 0
        dst_total_bytes += 0 if f is None else n.size
        dst.addfile(n, f)
        continue
    stats_js = {
        'date': {
            'epoch': time.mktime(time.localtime()),
            'local': time.strftime('%Y.%m.%d %H:%M:%S', time.localtime()),
            'UTC': time.strftime('%Y.%m.%d %H:%M:%S', time.gmtime())
        },
        'conversion': {
            'elapse_s': int(time.time() - t1)
        },
        'source': {
            'files':   src_files_count,
            'folders': src_folders_count,
            'others':  src_others_count,
            'links':   src_links_count,
            'total_items': src_total_items,
            'total_bytes': src_total_bytes
        },
        'suppressed': {
            'count': len(suppressed),
            'paths': sorted(list(suppressed)),
        },
        'links': {
            'hard':           hard_links,
            'hard_count':     len(hard_links),
            'symbolic':       symbolic_links,
            'symbolic_count': len(symbolic_links),
        },
        'destination': {
            'files':       dst_files_count,
            'folders':     dst_folders_count,
            'others':      dst_others_count,
            'links':       dst_links_count,
            'total_items': dst_total_items,
            'total_bytes': dst_total_bytes
        }
    }
    stats_bytes = json.dumps(stats_js, indent=4, sort_keys=True).encode('utf8')
    with open(dst_tar + '.json', 'wb') as f:
        f.write(stats_bytes)
    js_info = tarfile.TarInfo('preptar.json')
    js_info.size = len(stats_bytes)
    dst.addfile(js_info, io.BytesIO(stats_bytes))
    dst.close()
    return


# Example:
# > tar_filter.py
if __name__ == '__main__':
    source = 'some.tar.xz'
    abs_source = os.path.abspath(source)
    abs_destination = os.path.abspath(file_stem(source) + '.tar')
    main(abs_source, abs_destination)
