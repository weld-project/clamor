import requests

url = 'http://overpass-api.de/api/interpreter?data='
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["aeroway"="aerodrome"](24.059,-130,49.529,-60););out meta;'
qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["aeroway"="aerodrome"];);out meta;'
result = requests.get(url + qstr)

with open('queries-world.csv', 'w') as f:
    f.write(result.text)
