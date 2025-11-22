import requests
import pandas as pd
from datetime import datetime

url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()

df = pd.json_normalize(data)
df.to_csv(f"data/raw/covid_raw_{datetime.today().date()}.csv", index=False)
