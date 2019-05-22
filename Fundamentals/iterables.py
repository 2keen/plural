words = 'Why sometimes I have believed as many as six impossible things before breakfast'.split()

#list format: [expr(item) for item in iterable]
word_lengths = [len(word) for word in words]

#equivalent
lenghts = []
for word in words:
    lengths.append(len(word))
    
from math import factorial
f = [len(str(factorial(x))) for x in range(20)]
print('x\tdigits\tfactorial')
for xi, x in enumerate(f):
    print('{}\t{}\t{}'.format(xi, x, factorial(x)))


#set format: {expr(item) for item in iterable}
lengths_set = {len(word) for word in words}
lengths_set


#dictionary comprehensions
from pprint import pprint as pp
country_to_capital = {'United Kingdom': 'London',
                      'Brazil': 'Brazilia',
                      'Morocco': 'Rabat',
                      'Sweeden': 'Stockholm'}

capital_to_country = {capital: country for country, capital in country_to_capital.items()}
pp(country_to_capital)
pp(capital_to_country)

#only last value kept
words = ['hi', 'hello', 'foxtrot', 'hotel']
{x[0]: x for x in words}


from math import sqrt

def is_prime(x):
    if x < 2:
        return False
    for i in range(2, int(sqrt(x)) + 1):
        if x % i == 0:
            return False
    return True

[x for x in range(101) if is_prime(x)]

prime_square_divisors = {x*x:(1, x, x*x) for x in range(101) if is_prime(x)}
prime_square_divisors