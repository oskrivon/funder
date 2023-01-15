import yaml
from datetime import datetime, time, timedelta


with open('config.yaml') as f:
    cfg = yaml.safe_load(f)

delta = 2

print(cfg)

now = datetime.now()

funding_time = datetime.strptime(cfg['funding_times'][1], "%H:%M:%S")
funding_date = datetime(now.year, now.month, now.day, funding_time.hour, funding_time.minute, funding_time.second)
print(funding_date)

td = funding_date - now
print(td)
#entry_time = time(funding_time.hour - 1, 59, 60 - delta)
#print(entry_time)

print((datetime.strptime(cfg['funding_times'][0], "%H:%M:%S") - timedelta(seconds=2)).time())