# reducer.py
import sys

total = 0
count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t')
    total += float(value)
    count += 1

if count > 0:
    print(f"Average Age:\t{total/count}")
else:
    print("No data to process")
