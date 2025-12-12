import requests

API_KEY = "YOUR_REAL_API_KEY"
lat, lon = 40.7831, -73.9712

r = requests.get("https://api.openweathermap.org/data/2.5/weather",
                 params={"lat": lat, "lon": lon, "appid": API_KEY, "units": "imperial"})
print(r.status_code)
print(r.json())