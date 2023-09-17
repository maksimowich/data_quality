from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

start = date(2023, 2, 28).replace(day=1)
end = (date(2023, 2, 28) + relativedelta(months=1)).replace(day=1) - timedelta(days=1)

print(f"start {start}")
print(f"end {end}")
