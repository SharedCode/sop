import urllib.request

html = urllib.request.urlopen("http://localhost:8080/").read().decode('utf-8')
if "Database Options" in html:
    print('Found "Database Options" in HTML response')
else:
    print('NOT FOUND')
