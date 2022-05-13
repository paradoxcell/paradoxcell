'''
Basic parser
'''

import os
import pandas as pd

class DataFile():
    def __init__(self,dir_path,file_name,file_encoding,separated_values) -> None:
        self.dir_path = dir_path
        self.file_name = file_name
        self.file_encoding = file_encoding
        self.separated_values = separated_values
        self.file_path = self.get_path()
    
    def get_path(self):
        file_path = list()
        for (path, dir, files) in os.walk(self.dir_path):
            for file in files:
                if file.find(self.file_name) != -1:
                    file_path.append('/'.join([path,file]))
        return sorted(file_path)

    def get_df_readcsv(self):
        df = pd.DataFrame()
        for file in self.file_path:
            df_file = pd.read_csv(file,sep='\t')
            df = pd.concat([df,df_file],ignore_index=True)
        return df

    def get_df_open(self,column_sorted=False):
        df = pd.DataFrame()
        for file in self.file_path:
            f = open(file,encoding=self.file_encoding)
            data = f.read()
            f.close()
            df_file = pd.concat([pd.DataFrame([i.split(self.separated_values)]) for i in data.split('\n')],ignore_index=True)
            df = pd.concat([df,df_file],ignore_index=True)
        df.replace('',None,inplace=True)
        df.dropna(how='all',inplace=True)
        df.dropna(axis=1,how='all',inplace=True)
        if column_sorted != False:
            df.sort_values(by=[column_sorted],axis=0,ignore_index=True,inplace=True)
        else:
            pass
        return df
