#  tests for python.app-2-py36_8 (this is a generated file);
print('===== testing package: python.app-2-py36_8 =====');
print('running run_test.py');
#  --- run_test.py (begin) ---
import sys
from subprocess import check_call

print('-----')
x = sys.prefix + '/python.app/Contents/MacOS/python'
check_call([x, '-V'])
check_call([x, '-c', 'import sys; print(sys.prefix)'])

if sys.version_info[:2] >= (2, 7):
    from subprocess import check_output

    out = check_output([sys.prefix + '/bin/pythonw', 't.py', '1 2', '3'])
    print(out)
    assert eval(out.decode()) == ['t.py', '1 2', '3'], out
#  --- run_test.py (end) ---

print('===== python.app-2-py36_8 OK =====');
