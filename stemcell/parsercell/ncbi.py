from Bio import Entrez
from tqdm import tqdm
import pandas as pd

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