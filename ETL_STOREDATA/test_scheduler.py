import datetime

with open("scheduler_test.txt", "a") as f:
    f.write(f"Scheduler ran at {datetime.datetime.now()}\n")