#  tests for xlrd-1.1.0-py36h336f4a2_1 (this is a generated file);
print('===== testing package: xlrd-1.1.0-py36h336f4a2_1 =====');
print('running run_test.py');
#  --- run_test.py (begin) ---
import xlrd

wb = xlrd.open_workbook("test.xlsx")
sheet = wb.sheet_by_name("Sheet1")
cell = sheet.cell(0, 0)
print(cell.value)
assert(cell.value == "Hello, World!")
#  --- run_test.py (end) ---

print('===== xlrd-1.1.0-py36h336f4a2_1 OK =====');
print("import: 'xlrd'")
import xlrd

