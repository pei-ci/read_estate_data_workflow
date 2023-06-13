import requests
import os
from concurrent.futures import ThreadPoolExecutor


class RealEstateDataScraper:
    def __init__(self, real_estate_type_city_dict, begin_year, end_year):
        self.real_estate_type_city_dict = real_estate_type_city_dict
        self.begin_year = begin_year
        self.end_year = end_year

    def real_estate_scraper(self, args):
        year, season, real_estate_type, city = args
        if year > 1000:
            year -= 1911
        year_str = str(year)
        season_str = str(season)
        res = requests.get(
            'https://plvr.land.moi.gov.tw//DownloadSeason?season='+year_str+'S'+season_str+'&fileName='+city+'_lvr_land_'+real_estate_type+'.csv')
        all_data_folder = '../real_estate_data/'
        season_data_folder = '../real_estate_data/'+year_str+'_'+season_str+'/'
        create_folder_if_not_exists(all_data_folder)
        create_folder_if_not_exists(season_data_folder)
        fname = season_data_folder+city+'_lvr_land_'+real_estate_type+'.csv'
        open(fname, 'wb').write(res.content)

    def parallel_real_estate_scraper(self):
        args = []
        for year in range(self.begin_year, self.end_year+1):
            for season in range(1, 5):
                for real_estate_type in self.real_estate_type_city_dict:
                    for city in self.real_estate_type_city_dict[real_estate_type]:
                        args.append((year, season, real_estate_type, city))

        # 使用多進程進行平行化處理
        with ThreadPoolExecutor() as executor:
            executor.map(self.real_estate_scraper, args)


def create_folder_if_not_exists(folder_path):
    if not os.path.isdir(folder_path):
        os.mkdir(folder_path)


if __name__ == '__main__':
    real_estate_type_city_dict = {'A': ['A', 'E', 'F'], 'B': ['B', 'H']}
    begin_year = 106
    end_year = 109
    scraper = RealEstateDataScraper(
        real_estate_type_city_dict, begin_year, end_year)
    scraper.parallel_real_estate_scraper()
