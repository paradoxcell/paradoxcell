import urllib.parse
import urllib.request
import io

import pandas as pd

def get_df_uniprot_mapping(param_from,param_to,query_str):
    url = 'https://www.uniprot.org/uploadlists/'
    params = {
    'from': param_from,
    'to': param_to,
    'format': 'tab',
    'query': query_str
    }

    data = urllib.parse.urlencode(params)
    data = data.encode('utf-8')
    req = urllib.request.Request(url, data)
    with urllib.request.urlopen(req) as f:
        response = f.read()
    data = io.StringIO(response.decode('utf-8'))
    df = pd.read_csv(data,sep='\t')
    return df