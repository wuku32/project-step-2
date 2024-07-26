import requests

url = 'https://raw.githubusercontent.com/wuku32/project-step-2/master/cargohose_herren/2024_07/a/2024_07_26/FBE230705004_0447369901376482770.jpg'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}

response = requests.get(url, headers=headers)
print(response.status_code)

with open('1.jpg', 'wb') as f:
    f.write(response.content)
