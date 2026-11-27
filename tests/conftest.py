import csv
from datetime import datetime, time, timedelta

import pytest

from renewflow import eia930_csv as csv_source


@pytest.fixture
def write_days():
    def write(directory, label, days, *, value="2", omit_wind=False):
        with (directory / f"{label}-balance.csv").open("w", newline="") as balance, (
            directory / f"{label}-zone.csv"
        ).open("w", newline="") as subregion:
            ba = csv.writer(balance)
            zones = csv.writer(subregion)
            ba.writerow([csv_source.AUTHORITY, csv_source.HOUR_END, *csv_source.COLUMNS.values(), *csv_source.BATTERY])
            zones.writerow([csv_source.AUTHORITY, csv_source.HOUR_END, csv_source.ZONE, csv_source.DEMAND])
            for day in days:
                for hour in range(1, 25):
                    end = datetime.combine(day, time.min) + timedelta(hours=hour)
                    period = end.strftime("%m/%d/%Y %I:%M:%S %p")
                    for authority, codes in csv_source.ZONES.items():
                        wind = "" if omit_wind and hour == 24 and authority == "CISO" else value
                        ba.writerow([authority, period, "-1", wind, "4", "", "0"])
                        zones.writerows((authority, period, zone, "0") for zone in codes)
    return write
