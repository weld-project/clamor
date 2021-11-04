import requests

url = 'http://overpass-api.de/api/interpreter?data='
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["leisure"="park"](24.059,-130,49.529,-60););out meta;'
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["amenity"="restaurant"](36.3,-123,39,-120););out meta;'
#qstr_eur = '[out:csv(::lat, ::lon,"name";true;",")];(node["tourism"="hotel"](36.738884,16.611328,57.397624,29.267578););out meta;'
qstr_us = '[out:csv(::lat, ::lon,"name";true;",")];(node["amenity"="restaurant"](8.059230,-136.054688,61.270233,-59.765625););out meta;'
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["aeroway"="aerodrome"];);out meta;'
result = requests.get(url + qstr_us)

with open('queries-us-restaurants.csv', 'w') as f:
    f.write(result.text)
