import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin
from datetime import datetime
import re,calendar,time

start_time = time.time()

def get_city_weather_link(city_name, base_url):
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    link_groups = soup.find_all('div', class_='link-group')
    for group in link_groups:
        city_links = group.find('ul', class_='link-group-list').find_all('a')
        for link in city_links:
            if city_name.lower() in link.find('span', class_='link-text').get_text().lower():
                city_url = urljoin(base_url, link['href'])
                return city_url

    return None

def scrape_weather_data_for_week(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        weather_data = {}

        day_containers = soup.find_all("div", class_="day-row")

        for day_container in day_containers:
            day_of_week_tag = day_container.find('h3', class_='tab-day')
            time_tag = day_of_week_tag.find('time')
            day_of_week = time_tag['title'].split()[0] 

            time_tag = day_container.find('time')
            if time_tag:
                date_value = time_tag['datetime']

                temp_data = day_container.find_all('span', {'data-value': True})
                max_temp = temp_data[0].text.strip('°') if temp_data and len(temp_data) > 0 else 'N/A'
                min_temp = temp_data[1].text.strip('°') if temp_data and len(temp_data) > 1 else 'N/A'

                weather_data[day_of_week] = {'date': date_value, 'low': max_temp, 'up': min_temp}
            else:
                weather_data[day_of_week] = {'date': 'N/A', 'low': 'N/A', 'up': 'N/A'}

        return weather_data
    else:
        return {}





def scrape_weather_data_havadurumux(url):
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.find('table', id='hor-minimalist-a')
        
        if table is None:
            return {} 
        
        rows = table.find_all('tr')
        
        days = ["Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi", "Pazar"]
        weather_data = {}
        
        for i, day in enumerate(days):
            for row in rows:
                if day in row.text:
                    date = row.find('td').text.split(',')[0]
                    max_temp = row.find_all('td')[2].text.strip('°')
                    min_temp = row.find_all('td')[3].text.strip('°')

                    weather_data[calendar.day_name[i]] = {
                        'date': date,
                        'low': max_temp,
                        'up': min_temp
                    }
                    break 
            else:
                weather_data[calendar.day_name[i]] = {
                    'date': 'N/A',
                    'max_temp': 'N/A',}

        return weather_data


locations = []

province_data = {}
with open('provinces_code.txt', 'r', encoding='utf-8') as file:
    for line in file:
        parts = line.split('\t')
        if len(parts) >= 3:
            code = parts[0]
            name = parts[2]
            province_data[code] = name.strip()
            locations.append(name)

for code, name in province_data.items():
    province_url_metoffice = get_city_weather_link(name, 'https://www.metoffice.gov.uk/weather/world/turkey/')
    province_url_havadurumux = f'https://www.havadurumux.net/{name}-hava-durumu/'

    weather_data_metoffice = {}
    if province_url_metoffice:
        weather_data_metoffice = scrape_weather_data_for_week(province_url_metoffice)
        
    weather_data_havadurumux = {}
    if province_url_havadurumux:
        weather_data_havadurumux = scrape_weather_data_havadurumux(province_url_havadurumux)

    client = MongoClient('localhost', 27017)
    db = client['Aigerim_Raieva']
    collection = db['week_weather']

    weather_data_to_insert = {
        'provincial_plate': code,
        'weather': {
            'metoffice': weather_data_metoffice,
            'havadurumux': weather_data_havadurumux
        }
    }

    collection.insert_one(weather_data_to_insert)

