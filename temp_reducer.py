import sys

current_station = None
max_temp = -9999

for line in sys.stdin:
    station, temp = line.strip().split("\t")
    temp = int(temp)

    if current_station == station:
        if temp > max_temp:            max_temp = temp
    else:
        if current_station:
            print(f"{current_station}\t{max_temp}")
        current_station = station
        max_temp = temp

if current_station:
    print(f"{current_station}\t{max_temp}")

