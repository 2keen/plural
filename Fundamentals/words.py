from urllib.request import urlopen

def fetch_words():
    with urlopen('http://sixty-north.com/c/t.txt') as story:
        story_words = []
        for line in story:
            line_words = line.decode('utf-8').split()
            for word in line_words:
                story_words.append(word)

    words = ''        
    for word in story_words:
        words = words + ' ' + word
        
    print(words)

def square(x):
    return x * x
    
def launch_missiles():
    print('Missiles Launched!!!')
    
print(__name__)

if __name__ == '__main__':
    fetch_words()