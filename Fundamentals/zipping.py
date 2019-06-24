sunday = [12, 14, 15, 15, 17, 21, 22, 22, 23, 22, 20, 18]
monday = [13, 14, 14, 14, 16, 20, 21, 22, 22, 21, 19, 17]

def zip_print():
    for item in zip(sunday, monday):
        print(item)

def averages():
    for sun, mon in zip(sunday, monday):
        print('average: ', (sun + mon) / 2)    

averages()