import sys

for line in sys.stdin:
    parts = line.strip().split()
    if len(parts) == 3:
        station, year, temp = parts
        try:
            temp = int(temp)
            print(f"{station}\t{temp}")
        except ValueError:
            continue
