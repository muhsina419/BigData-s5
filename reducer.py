import sys

current_word = None
count = 0

for line in sys.stdin:
    word, num = line.strip().split("\t")
    num = int(num)

    if current_word == word:
        count += num
    else:
        if current_word:
            print(f"{current_word}\t{count}")
        current_word = word
        count = num

if current_word:
    print(f"{current_word}\t{count}")

