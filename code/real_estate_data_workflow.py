import os
import pandas as pd
import numpy as np
import cn2an


class Data:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.data_path_df = None
        self.data_df_list = None
        self.__check_folder_path_format()
        self.__generate_data_path_df()

    def __check_folder_path_format(self):
        '''
        檢查folder_path的格式，不符則進行轉換
        '''
        if self.folder_path[-1] not in ['/', '\\']:
            self.folder_path += '/'
        elif self.folder_path[-1] == '\\':
            self.folder_path = self.folder_path.replace('\\', '/')

    def __generate_data_path_df(self):
        data_path_dict = {"Folder": [], "File": []}
        # 取得第一層依照年與季分類的folders
        data_folder_list, else_file_list = get_folder_contents(
            self.folder_path)
        data_path_dict["Folder"] = data_folder_list

        # 取的folder中的檔案
        for folder in data_folder_list:
            else_folder_list, data_file_list = get_folder_contents(folder)
            # 只取名稱包含lvr_land的files
            lvr_land_data_file_list = [
                file for file in data_file_list if "lvr_land" in file]
            data_path_dict["File"].append(lvr_land_data_file_list)
        # 將dict轉df
        data_path_df = pd.DataFrame(data_path_dict)
        self.data_path_df = data_path_df

    def process_data(self):
        '''
        讀取csv檔、處理資料(設定column name、新增df_name欄位)
        '''
        data_df_list = []
        folder_num = len(self.data_path_df)
        # 處理每個folder
        for i in range(folder_num):
            folder_name = get_name_from_path(self.data_path_df.Folder[i])
            # 處理每個file與file的dataframe
            file_num = len(self.data_path_df.File[i])
            for j in range(file_num):
                data_file_path = self.data_path_df.File[i][j]
                file_name = get_name_from_path(data_file_path)
                # 讀取csv檔成dataframe格式
                try:
                    data_df = pd.read_csv(data_file_path, low_memory=False)
                except Exception as e:
                    isempty = os.stat(data_file_path).st_size == 0
                    if isempty:
                        continue
                    else:
                        print(e)
                    continue
                # 將csv第二列(在data_df中為第0列)的英文設為col的標頭，並刪除這個row
                data_df = data_df.rename(columns=data_df.iloc[0])
                data_df = data_df.drop(data_df.index[0])
                # 增加df_name欄位，並補上內容
                data_df['df_name'] = folder_name + '_' + \
                    file_name[0] + '_' + file_name[-5]
                data_df_list.append(data_df)
        self.data_df_list = data_df_list

    def __concatenate_data_df(self):
        '''
        將data_df全部合併
        '''
        # 合併dataframe，並將index reset 避免重複的index問題
        concat_df = pd.concat(self.data_df_list, axis=0).reset_index(drop=True)
        # 去除沒有column name的column、因為空資料即而產生的column
        concat_df = concat_df.loc[:, concat_df.columns.notna()]
        concat_df = concat_df.drop(['	<tr>'], axis=1)
        # 去除沒有資料的row
        concat_df = concat_df.loc[concat_df['land sector position building sector house number plate'].notna(
        )]
        return concat_df

    def get_data_df_list(self):
        return self.data_df_list

    def generate_df_all(self):
        '''
        產生df_all並回傳
        '''
        df_all = self.__concatenate_data_df()
        return df_all


def get_folder_contents(folder_path):
    '''
    取得folder中的文件和資料夾資訊
    '''
    folder_list = []
    file_list = []
    # 查看folder path
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        contents = os.listdir(folder_path)
        # 將folder中的資料夾與檔案分別加入list
        for item in contents:
            item_path = os.path.join(folder_path, item)
            if os.path.isdir(item_path):
                folder_list.append(item_path)
            elif os.path.isfile(item_path):
                file_list.append(item_path)
    else:
        print("Folder does not exist")
    return folder_list, file_list


def get_name_from_path(path):
    '''
    取得檔案名稱
    '''
    name = os.path.basename(path)
    return name


def convert_total_floor_to_number(floor):
    '''
    將樓層從中文轉數字
    '''
    if type(floor) == str:
        # 去掉"層"
        floor = floor.replace('層', '')
        # 透過cn2an將中文數字轉阿拉伯數字
        try:
            floor_num = int(cn2an.cn2an(floor, 'smart'))
        except:
            if floor == '地下':
                floor_num = -1
            else:
                floor_num = 0
    else:
        floor_num = 0
    return floor_num


def filter_df(df):
    '''
    將df透過指定條件篩選
    '''
    # 新增一列total floor number (number)紀錄數字型態的floor number，將用於條件篩選
    df['total floor number (number)'] = df['total floor number'].apply(
        convert_total_floor_to_number)
    # 根據條件篩選
    filt = (df['main use'] == '住家用') & (df['building state'].str.contains(
        '住宅大樓', na=False)) & (df['total floor number (number)'] >= 13)
    # 將剛剛新增的total floor number (number)刪除
    df = df.drop('total floor number (number)', axis=1)
    # filter
    filt_df = df.loc[filt]
    return filt_df


def generate_filter_csv(df):
    '''
    取得篩選結果，並儲存
    '''
    result_df = filter_df(df)
    save_df_to_csv(result_df, '../result/', 'filter.csv')


def count_df(df):
    '''
    計算df指定項目資訊
    '''
    # 前處理，將資料從str轉int
    df['total price NTD (number)'] = df['total price NTD'].apply(
        lambda x: int(x) if pd.notnull(x) else np.nan)
    df['the berth total price NTD (number)'] = df['the berth total price NTD'].apply(
        lambda x: int(x) if pd.notnull(x) else np.nan)

    # 全部資料
    # 計算總數與總價平均
    total_num = len(df)
    avg_price = df['total price NTD (number)'].mean()

    # 車位資料
    # 篩選包含車位的資料，只要包含車位就算
    berth_filt = (df['transaction sign'].str.contains('車位', na=False))
    berth_df = df.loc[berth_filt]
    # 計算總數與總價平均
    total_berth_num = len(berth_df)

    # 篩選有紀錄價格的車位
    priced_berth_filt = (df['the berth total price NTD (number)'] > 0)
    priced_berth_df = df.loc[priced_berth_filt]
    avg_berth_price = priced_berth_df['the berth total price NTD (number)'].mean(
    )

    df = df.drop(['the berth total price NTD (number)',
                 'total price NTD (number)'], axis=1)

    result = [{"total number": total_num, "the berth total number": total_berth_num,
              "average price NTD": avg_price, "the berth average price NTD": avg_berth_price}]
    count_df = pd.DataFrame(result)
    return count_df


def generate_count_csv(df):
    '''
    取得計算結果，並儲存
    '''
    result_df = count_df(df)
    save_df_to_csv(result_df, '../result/', 'count.csv')


def save_df_to_csv(df, folder_path, file_name):
    '''
    將df儲存成csv檔
    '''
    full_path = os.path.join(folder_path, file_name)
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    df.to_csv(full_path, encoding="utf_8_sig")


if __name__ == '__main__':
    # 題2
    data = Data('../real_estate_data/')
    print('data processing...')
    data.process_data()
    # 題3
    print('generating df_all...')
    df_all = data.generate_df_all()
    # 題4
    print('filtering df_all...')
    generate_filter_csv(df_all)

    print('counting df_all...')
    generate_count_csv(df_all)
    print('finish')
