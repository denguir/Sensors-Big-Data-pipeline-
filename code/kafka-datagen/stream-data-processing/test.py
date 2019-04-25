from configparser import ConfigParser

parser = ConfigParser()
parser.read('municipality.ini')
print(parser['municipalities']['1'])
