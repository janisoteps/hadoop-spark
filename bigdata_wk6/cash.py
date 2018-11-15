import csv

with open('sample10000.csv',  newline='') as file:
    reader = csv.reader(file, delimiter=',')
    cash_count = 0
    row_count = 0
    for row in reader:
        if row_count == 0:
            print(row[9])
        row_count += 1
        # print(row)
        if row[9] == '2':
            cash_count += 1

    print(cash_count)
