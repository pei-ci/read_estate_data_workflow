import requests
import os
from concurrent.futures import ThreadPoolExecutor


class RealEstateDataScraper:
    def __init__(self):
        self.url = 'https://plvr.land.moi.gov.tw/'

    def __real_estate_scraper(self, args):
        '''
        爬取指定年份、季度、類別、城市的資料
        '''
        year, season, real_estate_type, city = args
        if year > 1000:
            year -= 1911
        year_str = str(year)
        season_str = str(season)
        # 將args填入url中，並送出request
        res = requests.get(self.url+'/DownloadSeason?season='+year_str+'S' +
                           season_str+'&fileName='+city+'_lvr_land_'+real_estate_type+'.csv')
        # 最上層的資料夾，存放不同年份季度的資料夾]
        all_data_folder = '../real_estate_data/'
        # 下一層的資料夾，存放該年分季度的csv檔
        season_data_folder = '../real_estate_data/'+year_str+'_'+season_str+'/'
        create_folder_if_not_exists(all_data_folder)
        create_folder_if_not_exists(season_data_folder)

        # 將透過request取得的內容寫入csv檔
        fname = season_data_folder+city+'_lvr_land_'+real_estate_type+'.csv'
        open(fname, 'wb').write(res.content)

    def parallel_real_estate_scraper(self, real_estate_type_city_dict, begin_year, end_year):
        '''
        平行爬取資料
        '''
        # 產生所有arg組合的set
        args = []
        for year in range(begin_year, end_year+1):
            for season in range(1, 5):
                for real_estate_type in real_estate_type_city_dict:
                    for city in real_estate_type_city_dict[real_estate_type]:
                        args.append((year, season, real_estate_type, city))

        # 使用多進程進行平行化處理
        with ThreadPoolExecutor() as executor:
            executor.map(self.__real_estate_scraper, args)


def create_folder_if_not_exists(folder_path):
    '''
    如果folder不存在，則建立folder
    '''
    if not os.path.isdir(folder_path):
        os.mkdir(folder_path)


if __name__ == '__main__':
    # 索引表示不動產的類型，索引內容列出城市
    real_estate_type_city_dict = {'A': ['A', 'E', 'F'], 'B': ['B', 'H']}
    begin_year = 106
    end_year = 109
    # 建立爬蟲物件
    scraper = RealEstateDataScraper()
    # 平行爬取資料
    scraper.parallel_real_estate_scraper(
        real_estate_type_city_dict, begin_year, end_year)
