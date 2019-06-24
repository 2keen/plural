

def lucas():
    yield 2
    a = 2
    b = 1
    while True:
        yield b
        a, b = b, a + b


l = lucas()
for i in range(10):
    print('{}: {}'.format(i,next(l)))