#  tests for ipykernel-4.8.2-py36_0 (this is a generated file);
print('===== testing package: ipykernel-4.8.2-py36_0 =====');
print('running run_test.py');
#  --- run_test.py (begin) ---
import json
import os
import sys


py_major = sys.version_info[0]
specfile = os.path.join(os.environ['PREFIX'], 'share', 'jupyter', 'kernels',
                        'python{}'.format(py_major), 'kernel.json')
with open(specfile, 'r') as fh:
    spec = json.load(fh)


if spec['argv'][0] != sys.executable:
    raise ValueError('The specfile seems to have the wrong prefix. \n'
                     'Specfile: {}; Expected: {};'
                     ''.format(spec['argv'][0], sys.executable))
#  --- run_test.py (end) ---

print('===== ipykernel-4.8.2-py36_0 OK =====');
print("import: 'ipykernel'")
import ipykernel

