a = 496
print('value of a: ' + str(a))
print('id of a: ' + str(id(a)))

b=1729
print()
print('value of b: ' + str(b))
print('id of b: ' + str(id(b)))

b = a
print()
print('set b = a')
print('value of b: ' + str(b))
print('id of b: ' + str(id(b)))
print('a is b test: ')
print(a is b)

b = 496
print()
print('set b = 496')
print('id of b: ' + str(id(b)))
print('a is b test: ')
print(a is b)


t=5
print('t=5')
print('t id: ' + str(id(t)))
t+=2
print('t+=2')
print('t id: ' + str(id(t)))

r = [2, 4, 6]
s = r
s[1] = 17
s is r

p = [4, 7, 11]
q = [4, 7, 11]
p == q
p is q

def add_spam(menu=[]):
    menu.append('spam')
    return menu
    
breakfast = ['bacon', 'eggs']
add_spam(breakfast)
lunch = ['baked beans']
add_spam(lunch)
add_spam()
add_spam()
add_spam()

def add_spam(menu=None):
    if menu == None:
        menu = []
    menu.append('spam')
    return menu