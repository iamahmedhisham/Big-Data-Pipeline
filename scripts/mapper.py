# mapper.py
import sys
from datetime import datetime

def calculate_age(dob):
    birth_date = datetime.strptime(dob, '%Y-%m-%d')
    today = datetime.today()
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 3:
        continue
    dob = fields[2]  
    try:
        age = calculate_age(dob)
        print(f"1\t{age}")
    except:
        continue
