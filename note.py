import os
import pandas as pd
from collections import Counter
from Bio import Entrez
from tqdm import tqdm
import pandas as pd
import xml.etree.ElementTree as ET
import pandas as pd

def Parse_sra_ExpXml(x):
    '''
    len(child.tags) = 10
    child.tags = [
        Summary,
        Submitter,
        Experiment,
        Study,
        Organism,
        Sample,
        Instrument,
        Library_descriptor,
        Bioproject,
        Biosample
        ]
    '''
    tree = '<root>' + x + '</root>'
    root = ET.fromstring(tree)

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

def dic_fna(fnafile):
    dic = {}
    openfile_fna = open(fnafile)
    readfile_fna = openfile_fna.read()
    for each in readfile_fna.split('>')[1:]:
        strHeader      = each.split('\n')[0].split()[0]
        strSeq         = ''.join(each.split('\n')[1:])
        dic[strHeader] = strSeq
    return dic


def list_BioprojectId_to_df_bioproject(list_BioprojectId):
    df_bioproject = pd.DataFrame()
    df_error_bioproject = pd.DataFrame()
    for i in tqdm(list_BioprojectId):
        try:
            Entrez.email = "sjt0332@gnu.ac.kr"
            handle = Entrez.esearch(db="bioproject", term=i, retmax='200')
            record = Entrez.read(handle)
            handle.close()
            handle = Entrez.efetch(db="bioproject", id=record['IdList'], rettype="docsum", retmode="xml")
            record = Entrez.read(handle)
            handle.close()
            df_tmp = pd.DataFrame([record['DocumentSummarySet']['DocumentSummary'][0]])
            df_bioproject = df_bioproject.append(df_tmp)
        except Exception as ex:
            dict_error_bioproject_tmp = {i:ex}
            df_error_bioproject_tmp = pd.DataFrame.from_dict(dict_error_bioproject_tmp, orient='index')
            df_error_bioproject = df_error_bioproject.append(df_error_bioproject_tmp)
            print(i," : ",ex)
    list_output_bioproject = [df_bioproject,df_error_bioproject]
    return list_output_bioproject

def list_BioprojectId_to_df_biosample(list_BioprojectId):
    df_biosample = pd.DataFrame()
    df_error_biosample = pd.DataFrame()
    for i in tqdm(list_BioprojectId):
        try:
            Entrez.email = "sjt0332@gnu.ac.kr"
            handle = Entrez.esearch(db="bioproject", term=i, retmax='200')
            record = Entrez.read(handle)
            handle.close()
            handle = Entrez.elink(dbfrom="bioproject", id=record['IdList'], linkname="bioproject_biosample")
            record = Entrez.read(handle)
            handle.close()
            linked = [link["Id"] for link in record[0]["LinkSetDb"][0]["Link"]]
            df_tmp0 = pd.DataFrame()
            for i in linked:
                handle = Entrez.efetch(db="biosample", id=i, rettype="docsum", retmode="xml")
                record = Entrez.read(handle)
                handle.close()
                df_tmp1 = pd.DataFrame([record['DocumentSummarySet']['DocumentSummary'][0]])
                df_tmp0 = df_tmp0.append(df_tmp1)
                df_biosample = df_biosample.append(df_tmp0)
        except Exception as ex:
            dict_error_biosample_tmp = {i:ex}
            df_error_biosample_tmp = pd.DataFrame.from_dict(dict_error_biosample_tmp, orient='index')
            df_error_biosample = df_error_biosample.append(df_error_biosample_tmp)
            print(i," : ",ex)
    list_output_biosample = [df_biosample,df_error_biosample]
    return list_output_biosample

def list_BioprojectId_to_df_sra(list_BioprojectId):
    df_sra = pd.DataFrame()
    df_error_sra = pd.DataFrame()
    for i in tqdm(list_BioprojectId):
        try:
            Entrez.email = "sjt0332@gnu.ac.kr"
            handle = Entrez.esearch(db="bioproject", term=i, retmax='200')
            record = Entrez.read(handle)
            handle.close()
            handle = Entrez.elink(dbfrom="bioproject", id=record['IdList'], linkname="bioproject_sra")
            record = Entrez.read(handle)
            handle.close()
            linked = [link["Id"] for link in record[0]["LinkSetDb"][0]["Link"]]
            df_tmp0 = pd.DataFrame()
            for i in linked:
                handle = Entrez.efetch(db="sra", id=i, rettype="docsum", retmode="xml")
                record = Entrez.read(handle)
                handle.close()
                df_tmp1 = pd.DataFrame([record[0]])
                df_tmp0 = df_tmp0.append(df_tmp1)
                df_sra = df_sra.append(df_tmp0)
        except Exception as ex:
            dict_error_sra_tmp = {i:ex}
            df_error_sra_tmp = pd.DataFrame.from_dict(dict_error_sra_tmp, orient='index')
            df_error_sra = df_error_sra.append(df_error_sra_tmp)
            print(i," : ",ex)
    list_output_sra = [df_sra,df_error_sra]
    return list_output_sra