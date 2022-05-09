import os
import pandas as pd
from collections import Counter

def get_df_masked(df_reference,column_key,mask,column_sorted=False):
    df_masked = pd.DataFrame()
    m = df_reference[column_key] == mask
    df_masked = df_masked.append(df_reference[m],ignore_index=True)
    if column_sorted != False:
        df_masked = df_masked.sort_values(by=[column_sorted],axis=0,ignore_index=True)
    else:
        pass
    return df_masked

def get_df_masked_by_id_is(df_reference,column_key,list_mask,column_id,column_sorted=False):
    df_masked_by_id = pd.DataFrame()
    set_masked_id = set()
    for i in list_mask:
        m = df_reference[column_key] == i
        set_masked_id.update(set(df_reference[m][column_id]))
    list_masked_id = list(set_masked_id)
    for k in list_masked_id:
        m = df_reference[column_id] == k
        df_masked_by_id = df_masked_by_id.append(df_reference[m],ignore_index=True)
    if column_sorted != False:
        df_masked_by_id = df_masked_by_id.sort_values(by=[column_sorted],axis=0,ignore_index=True)
    else:
        pass
    return df_masked_by_id

def get_df_masked_by_id_isnot(df_reference,column_key,list_mask,column_id,column_sorted=False):
    df_masked_by_id = pd.DataFrame()
    set_masked_id_isnot = set()
    for i in list_mask:
        m = df_reference[column_key] == i
        set_masked_id_isnot.update(set(df_reference[column_id]) - set(df_reference[m][column_id]))
    list_masked_id_isnot = list(set_masked_id_isnot)
    for k in list_masked_id_isnot:
        m = df_reference[column_id] == k
        df_masked_by_id = df_masked_by_id.append(df_reference[m],ignore_index=True)
    if column_sorted != False:
        df_masked_by_id = df_masked_by_id.sort_values(by=[column_sorted],axis=0,ignore_index=True)
    else:
        pass
    return df_masked_by_id

def get_list_df_counter_by_column(df_reference,column_key,column_value,limit=None):
    list_df_counter_by_column = list()
    for index,i in enumerate([i[0] for i in Counter(df_reference[column_key]).most_common()]):
        m = df_reference[column_key] == i
        list_df_counter_by_column.append([index,i,Counter(df_reference[m][column_value]).most_common(limit)])
    return list_df_counter_by_column


def get_df_part(df_reference,part,column_sorted=False):
    df_part = pd.DataFrame()
    m_part = df_reference[5] == part
    list_id = list(set(df_reference[m_part][2]))
    for i in list_id:
        m = df_reference[2] == i
        df_part = df_part.append(df_reference[m],ignore_index=True)
    if column_sorted != False:
        df_part = df_part.sort_values(by=[column_sorted],axis=0,ignore_index=True)
    else:
        pass
    return df_part

def get_list_df_masked(df_reference,mask,column_key,column_value):
    m = df_reference[column_key] == mask
    list_df_masked = list(set(df_reference[m][column_value]))
    return sorted(list_df_masked)

def get_counter_df_masked(df_reference,mask,column_key,column_value):
    m = df_reference[column_key] == mask
    counter_df_masked = Counter(df_reference[m][column_value]).most_common()
    return counter_df_masked