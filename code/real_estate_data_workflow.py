import os
import pandas as pd


class Data:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.check_folder_path()
        self.data_path_df = self.generate_data_path_df()
        self.data_df_list = self.load_data()
        self.df_all = self.concatenate_data_df()
        print(self.df_all)

    def check_folder_path(self):
        if self.folder_path[-1] == '\\':
            self.folder_path[-1] = '/'
        elif self.folder_path[-1] != '/':
            self.folder_path += '/'

    def get_folder_contents(self, folder_path):
        # Check if the folder exists
        folder_list = []
        file_list = []
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            # List the files and subfolders in the folder
            contents = os.listdir(folder_path)
            for item in contents:
                item_path = os.path.join(folder_path, item)
                if os.path.isdir(item_path):
                    folder_list.append(item_path)
                elif os.path.isfile(item_path):
                    file_list.append(item_path)
        else:
            print("Folder does not exist")
        return folder_list, file_list

    def generate_data_path_df(self):
        data_path_dict = {"Folder": [], "File": []}
        # 取得第一層依照年與季分類的folders
        data_folder_list, else_file_list = self.get_folder_contents(
            self.folder_path)
        data_path_dict["Folder"] = data_folder_list

        # 取的folder中的檔案
        for folder in data_folder_list:
            else_folder_list, data_file_list = self.get_folder_contents(folder)
            # 只取名稱包含lvr_land的files
            lvr_land_data_file_list = [
                file for file in data_file_list if "lvr_land" in file]
            data_path_dict["File"].append(lvr_land_data_file_list)
        # 將dict轉df
        data_path_df = pd.DataFrame(data_path_dict)
        return data_path_df

    def get_name_from_path(self, path):
        name = os.path.basename(path)
        return name

    def set_df_name(self, series, folder_name, file_name):
        df_name = folder_name + '_' + file_name[0] + '_' + file_name[-5]
        return df_name

    def load_data(self):
        data_df_list = []
        folder_num = len(self.data_path_df)
        file_num = 5
        for i in range(folder_num):
            folder_name = self.get_name_from_path(self.data_path_df.Folder[i])
            for j in range(file_num):
                data_file_path = self.data_path_df.File[i][j]
                file_name = self.get_name_from_path(data_file_path)
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
                data_df = data_df.rename(
                    columns=data_df.iloc[0]).drop(data_df.index[0])
                # 增加df_name欄位，並補上內容
                data_df['df_name'] = data_df.apply(
                    self.set_df_name, args=(folder_name, file_name, ), axis=1)

                data_df_list.append(data_df)
        return data_df_list

    def concatenate_data_df(self):
        df = pd.concat(self.data_df_list, axis=0)
        df = df.loc[:, df.columns.notna()]
        return df


if __name__ == '__main__':
    a = Data('../real_estate_data/')
# def load_data():
# load_data()
#isempty = os.stat('path\to\file\filename.ext').st_size == 0
