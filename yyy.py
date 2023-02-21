from collections import namedtuple
import requests
import json

Point = namedtuple('Point', ['x', 'y'])
p = Point(20, 85)
print(p.x)

class _HeaderStorage:
    def __init__(self):
        self.x = 10
        self.y = 20

header_storage = _HeaderStorage()

print(header_storage.x)

SUBDOMAIN = 'xxx'
DOMAIN = 'yyy'

PUBLIC_WSS = f"wss://{SUBDOMAIN}.{DOMAIN}.com/perpetual/ws/v1/realtime_public"

print(PUBLIC_WSS)

def xxx(**kwargs):
    print(kwargs)
    print(kwargs['xxx1'])

    if 'xxx1' in kwargs:
        print('kwarg consist')
    
    if 'xxx3' in kwargs:
        print('ok')
    else:
        print('not ok')

xxx(xxx1 = 125, xxx2 = ['a', 'b', 'c'], xxx3 = 'www/sdsd/sd')

ddd = {}

ddd['q'] = 120
ddd['e'] = 120

ddd['q'] = 200

print(ddd)