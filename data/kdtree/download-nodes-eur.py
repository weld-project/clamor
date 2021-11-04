import requests

url = 'http://overpass-api.de/api/interpreter?data='
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["addr:street"](24.059,-130,49.529,-60););out meta;'
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["addr:street"](24.059,-130,49.529,-100););out meta;'
bot = -45
top = 13
cur = -80
end = -30
# -45.1,-82.0,13.6,-34.2
# -35.5,10.8,15.6,43.6
# 36.4,-11.1,58.8,29.4
while cur < end:
    print(cur)
    qstr = '[out:csv(::lat, ::lon,"name";false;",")];(node["addr:street"](%d,%d,%d,%d););out meta;' % (bot, cur, top, cur+5)
    print(qstr)
    result = requests.get(url + qstr)
    txt = result.text
    print(len(txt))
    with open('nodes-sa.csv', 'a+') as f:
        f.write(txt)
        f.flush()
    cur = cur+5
#qstr = '[out:csv(::lat, ::lon,"name";true;",")];(node["building"];);out meta;'

