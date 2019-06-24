import math
import os
import sys
from pprint import pprint as pp

import requests

print(sys.version)
print(sys.executable)


def greet(who_to_greet):
    greeting = 'Hello {}'.format(who_to_greet)
    return greeting


name = input('Your Name? ')

print('Hello {}'.format(name))

print(greet('World'))
print(greet('Tommy'))

