import pandas as pd
import json
import os
import sys
from datetime import datetime


class MergeCsv:
    def __init__(self, directory, file_to_save):
        self.directory = directory
        self.file_to_save = file_to_save


    def open_json(self, filename: str) -> list[dict]:
            try:
                with open(filename) as file:
                    data = json.load(file)
            except Exception as e:
                data = []
                print(f'JSON file: {filename} is empty\n{e}')
            
            return data


    def get_city_info(self, city_id):
        city_list = self.open_json('cfo.list.json')

        for city in city_list:
            if int(city['id']) == city_id:
                city_name = city['name'] if 'name' in city.keys() else ''
                region = city['region'] if 'region' in city.keys() else ''
                lat = city['coord']['lat'] if 'coord' in city.keys() else 1000
                lon = city['coord']['lon'] if 'coord' in city.keys() else 1000
                return (city_name, region, lat, lon)


    def find_empty_info(self, city_name, region, lat, lon):
        fields = ''

        if city_name == '':
            fields += ' city_nam'
        elif region == '':
            fields += ' region'
        elif lat == 1000:
            fields += ' lat'
        elif lon == 1000:
            fields += ' lon'
        
        return fields.strip()


    def merge_csv_files(self):
        for i, filename in enumerate(os.listdir(self.directory)):
            if filename[-4:] != '.csv':
                continue

            try:
                df = pd.read_csv(f'{self.directory}/{filename}')
            except Exception as e:
                print(e)
                print(f'{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}: {i+1}/{len(os.listdir(self.directory))}: Error on {filename}')


            city_id = int(filename[:-4].split('_')[2])
            city_name, region, lat, lon = self.get_city_info(city_id)

            check_fields = self.find_empty_info(city_name, region, lat, lon)

            if check_fields:
                print(f'{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}: {i+1}/{len(os.listdir(self.directory))} Info "{self.find_empty_info(city_name, region, lat, lon)}" not found for file {filename}')

            df['year'] = pd.DatetimeIndex(df['date']).year
            df['month'] = pd.DatetimeIndex(df['date']).month
            df['day'] = pd.DatetimeIndex(df['date']).day
            df['hour'] = pd.DatetimeIndex(df['date']).hour
            df['city_id'] = city_id
            df['city_name'] = city_name
            df['region'] = region
            df['lat'] = lat
            df['lon'] = lon
            
            self.save_df(df, self.file_to_save)
    

    def save_df(self, df, filename):
        if not os.path.isfile(filename):
            df.to_csv(filename, mode='w', header=True)
        else:
            df.to_csv(filename, mode='a', header=False)


if __name__ == '__main__':
    #air = MergeCsv(directory='air_quality_by_city', file_to_save='air_data.csv')
    #air = MergeCsv(directory=sys.argv[1], file_to_save=sys.argv[2])
    #air.merge_csv_files()


    weather_csv = pd.read_csv('weather_data.csv')
    #air_csv = pd.read_csv('air_data.csv')

    print(weather_csv.shape)
    #final_df = pd.merge(weather_csv, air_csv, on=['date', 'year', 'month', 'day', 'hour', 'city_id','city_name', 'region', 'lat', 'lon'])
    #print(final_df.shape)
    #final_df.to_csv('air_weather_data.csv', index=False)
