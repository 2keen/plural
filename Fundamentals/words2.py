"""Commands to call various aspects of the module
Full Call from terminal:
    python C:/Users/txk3623/dev/python/plural/Fundamentals/words2.py http://sixty-north.com/c/t.txt

Local call from terminal after changing directory:
    cd ./dev/python/plural/Fundamentals
    python words2.py http://sixty-north.com/c/t.txt

Print the Docstring for fetch_words function:
    ipython
    import words2
    from words2 import fetch_words()
    help(fetch_words)
    help(words2)
"""

import sys
from urllib.request import urlopen

def fetch_words(url):
    """Fetch a list of words from a URL.
    
    Args:
        url: the URL of a UTF-8 text document
    
    Returns:
        A list of strings containing the words from the document
    """
    with urlopen(url) as story:
        story_words = []
        for line in story:
            line_words = line.decode('utf-8').split()
            for word in line_words:
                story_words.append(word)
    return story_words


def print_items(items):
    """Print items one per line.
    
    Args:
        An iterable series of printable items
    """
    for item in items:
        print(item)
        

def main(url='http://sixty-north.com/c/t.txt'):
    """Print each word from a text document from a URL
    
    Args:
        url: the URL of a UTF-8 text document
    """
    words = fetch_words(url)
    print_items(words)


if __name__ == '__main__':
    main(sys.argv[1]) # The 0th argument is the module filename
