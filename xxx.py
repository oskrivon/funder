import yaml
from datetime import datetime, timedelta
import time

import schedule

import entry_points_comput as epc


with open('config.yaml') as f:
    cfg = yaml.safe_load(f)

def job():
    print("I'm working...")

entry_points = epc.entry_points_comput(cfg['funding_times'], cfg['offset'])
print(entry_points)
for point in entry_points:
    schedule.every().day.at(point).do(job)
    print('ok')

while True:
    schedule.run_pending()
    time.sleep(1)