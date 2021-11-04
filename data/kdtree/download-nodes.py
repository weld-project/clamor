import requests

url = 'http://overpass-api.de/api/interpreter?data='
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["addr:street"](24.059,-130,49.529,-60););out meta;'
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["addr:street"](24.059,-130,49.529,-100););out meta;'
cur = -130
end = -60
while cur < end:
    print(cur)
    qstr = '[out:csv(::lat, ::lon,"name";false;",")];(node["amenity"="restaurant"](24.059,%d,49.529,%d););out meta;' % (cur, cur+10)
    result = requests.get(url + qstr)
    with open('queries-us-restaurants.csv', 'a+') as f:
        f.write(result.text)
    cur = cur+10
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["building"];);out meta;'

