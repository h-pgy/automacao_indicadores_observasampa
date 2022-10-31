#faz o download direto dos objetos do Portal de Dados Abertos por meio do link
import requests
import pandas as pd
from io import StringIO

class DownloadObjeto:
    '''Faz o download dos objetos do Portal de Dados Abertos
    por meio do link direto para baixar o objeto.
    
    Objeto deve ser final do link, após o parâmetro 'dataset'.

    Implementado: download de csvs.'''

    def __init__(self):

        self.dominio = 'http://dados.prefeitura.sp.gov.br/'
        self.endpoint = 'dataset/'

    def build_url_download(self, objeto):
    
        return self.dominio + self.endpoint + objeto

    def download_objeto_portal(self, objeto, content_type='text'):
    
        url = self.build_url_download(objeto)
        
        with requests.get(url) as r:
            assert r.status_code == 200
            if content_type == 'text':
                conteudo = r.text
            elif content_type == 'json':
                conteudo = r.json()
            else:
                conteudo = r.content
        
        return conteudo
    def download_csv_portal(self, objeto, encoding='latin-1',
                            sep=';'):
    
        conteudo = self.download_objeto_portal(objeto)
        io = StringIO(conteudo)
        return pd.read_csv(io, sep=sep, encoding=encoding)