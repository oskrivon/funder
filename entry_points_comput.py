
from datetime import datetime, timedelta


def entry_points_comput(ep_times, offset):
    now = datetime.now()
    funding_dates = []
    entry_dates = []
    for time_ in ep_times:
        funding_time = datetime.strptime(time_, "%H:%M:%S")
        funding_date = datetime(
            now.year, now.month, now.day, 
            funding_time.hour, funding_time.minute, funding_time.second
        )

        funding_dates.append(funding_date)
        entry_dates.append(funding_date - timedelta(seconds=offset))

    ep_times_str = []
    for date in entry_dates:
        ep_times_str.append(str(date.time()))

    return ep_times_str