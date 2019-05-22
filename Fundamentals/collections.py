"""Python collections
    str - string - immutable sequence of unicdoe code points
    list - list - mutable sequence of objects
    dict - dictionary - mutable mapping from immutable keys to mutable objects
    tuple - tuple - immutable sequence of objects
    range - range - arithmatic progression of integers
    set - set - mutaple collection of unique mutable objects
"""

#tuples
t = ('Norway', 4.953, 3) 
print('Tuples can contain mixed datatypes as shown below')
print('t = ('Norway', 4.953, 3)')
print(t)
print()
print('Tuple elements can be accessed via indexing')
print('t[0]: ' + str(t[0]))
print()
print('Lengths of tuples can be accessed via len()')
print('len(t): ' + str(len(t)))
print('concatenate with +')
print('repeat with *')
print('single element tuple require trailing comma: k = (391,)')
print('empty parentheses create empty tuple')
print()
print('Tuple Unpacking allows for destructuring of tuples to named references')
def minmax(items):
    return min(items), max(items)
print('minmax([83, 33, 84, 32, 85, 31, 86]): ' + str(minmax([83, 33, 84, 32, 85, 31, 86])))
lower, upper = minmax([83, 33, 84, 32, 85, 31, 86])
print('lower, upper = minmax([83, 33, 84, 32, 85, 31, 86])')
print('lower: ' + str(lower))
print('upper: ' + str(upper))
print()
(a, (b, (c, d))) = (4, (3, (2, 1)))
print('(a, (b, (c, d))) = (4, (3, (2, 1)))')
print('a: ' + str(a))
print('b: ' + str(b))
print('c: ' + str(c))
print('d: ' + str(d))
a, b = b, a
print('a, b = b, a')
print('a: ' + str(a))
print('b: ' + str(b))

#strings
print('join vs +')
colors = ';'.join(['rgb(0,0,0)', 'rgb(128,128,128)', 'rgb(64,64,64)', 'rgb(32,32,32)', 'rgb(16,16,16)'])
print("colors = ';'.join(['rgb(0,0,0)', 'rgb(128,128,128)', 'rgb(64,64,64)', 'rgb(32,32,32)', 'rgb(16,16,16)'])")
print(colors)
print("colors.split(';')")
print(colors.split(';'))
print()
print("''.join(['high', 'way', 'man'])")
print(''.join(['high', 'way', 'man']))
print('partition is used to split a string into 3')
print("'unforgetable'.partition('forget')")
print('unforgetable'.partition('forget'))


#list
a = [[1, 2], [3, 4]] #a references a[0]; a[0][0] references 1 object
b = a[:] #creates new list object with references to a[0] and a[1]
print('a is b: {}'.format(a is b)) #False
print('a == b: {}'.format(a == b)) #True
print('a[0]: {}'.format(a[0]))
print('b[0]: {}'.format(b[0]))
print('a[0] is b[0]: {}'.format(a[0] is b[0])) #True
print('a[0] = [8,9]')
a[0] = [8,9]
print('a[0]: {}'.format(a[0]))
print('b[0]: {}'.format(b[0]))
print('a[1].append(5)')
a[1].append(5)
print('a: {}'.format(a))
print('b: {}'.format(b))

print('c = [21, 37]')
c = [21, 37]
print('d = c * 4')
d = c * 4
print('d: {}'.format(d))
s = [[-1, +1]] * 5 #creates a list of 5 references to the 1 inner list
print('s: {}'.format(s))
print('s = [[-1, +1]] * 5')
s[3].append(7)
print('s[3].append(7)')
print('s: {}'.format(s))

#sets
s = {1,2,3,4}
e = set()
print(e)
s = [1, 4, 2, 1, 7, 9, 9]
t = set(s)
print(s)
print(t)
set(t)
t.add(54)
print(t)
t.add(54)
print(t)
t.update([55, 243, 186])
print(t)
t.remove(55)
print(t)
t.remove(55) #results in error
t.discard(1)
print(t)
t.discard(1) # no error
j = t.copy()
n = set(j)
print('{}\n{}\n{}'.format(t,j,n))

blue_eyes = {'Olivia', 'Harry', 'Lily', 'Jack', 'Amelia'}
blond_hair = {'Harry', 'Jack', 'Amelia', 'Mia', 'Joshua'}
smell_hcn = {'Harry', 'Amelia'}
taste_ptc = {'Harry', 'Lily', 'Amelia', 'Lola'}
o_blood = {'Mia', 'Joshua', 'Lily', 'Olivia'}
b_blood = {'Amelia', 'jack'}
a_blood = {'Harry'}
ab_blood = {'Joshua', 'Lola'}

blue_eyes.union(blond_hair) #either
blond_hair.union(blue_eyes) == blond_hair.union(blue_eyes)

blue_eyes.intersection(blond_hair) #both

blond_hair.difference(blue_eyes) #only first (blond hair not blue eyes)
blond_hair.difference(blue_eyes) == blue_eyes.difference(blond_hair)
blue_eyes.intersection(blond_hair) #both

blond_hair.symmetric_difference(blue_eyes) #either, not both

smell_hcn.issubset(taste_ptc)
a_blood.isdisjoint(o_blood) #no members in common