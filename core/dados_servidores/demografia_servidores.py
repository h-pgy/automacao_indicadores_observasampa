from core.utils.portal_dados_abertos.download_direto import DownloadObjeto
import json


OBJETO = ('bf5df0f4-4fb0-4a5e-b013-07d098cc7b1c/resource/'
        '8275d64c-7268-488a-8df6-abadbf114fe2/download/'
        'verificadoativos03-01-2022dez-2021.csv')

def gerar_json_cargos_comissao():

    download = DownloadObjeto()
    df = download.download_csv_portal(OBJETO)

    cargos = df[df['CARGO_COMISSAO'].notnull()]['CARGO_COMISSAO'].unique()

    with open('json_cargos.json', 'w') as f:
        json.dump({c : False for c in cargos}, f)


class RecorteServidores:
    '''Calcula indicadores relacionados à população de servidores ativos
    e comissionados da administração direta do executivo Municipal de São Paulo.
    
    São calculadas as seguintes variáveis:

    * Quantidade de servidores deficientes
    * QUantidade de servidores negros
    * Quantdade de servidoras mulheres
    * Quantidade de servidoras mulheres negras
    * Total de servidores ativos

    Dataset está hard-coded para agosto.
    '''

    def __init__(self):
    
        self.downloader =  DownloadObjeto()
        self.objeto = OBJETO

    def download_csv(self):

        return self.downloader.download_csv_portal(self.objeto)

    def drop_rf_duplo_vinculo(self, df):
    
        df = df[~df['REGISTRO'].duplicated()]
        df = df.copy().reset_index(drop=True)
        assert df['REGISTRO'].duplicated().any()==False
        
        return df

    def is_negro(self, df):
    
        df = df.copy()
        racas_negras = {'PRETA', 'PARDA'}
        
        df['negro'] = df['RACA'].isin(racas_negras)
        
        return df

    def is_mulher(self, df):
    
        df = df.copy()
        
        df['mulher'] = df['SEXO'] == 'F'
        
        return df

    def is_mulher_negra(self, df):
    
        df = df.copy()
        
        df['mulher_negra'] = (df['negro']&df['mulher'])
        
        return df

    def is_deficiente(self, df):
    
        df = df.copy()
        
        df['deficiente'] = df['DEFICIENTE']=='SIM'
        
        return df

    def pipeline_feature_engineering(self, df):

        df = self.is_negro(df)
        df = self.is_mulher(df)
        df = self.is_mulher_negra(df)
        df = self.is_deficiente(df)


        return df

    def calcular_variaveis(self, df):

        variaveis = {
            'total_servidores' : len(df),
            'total_mulheres' : df['mulher'].sum(),
            'total_negros' : df['negro'].sum(),
            'total_mulheres_negras' : df['mulher_negra'].sum(),
            'total_deficientes' : df['deficiente'].sum(),
        }


        return variaveis

    def __call__(self):

        df = self.download_csv()
        df = self.pipeline_feature_engineering(df)

        variaveis = self.calcular_variaveis(df)

        return variaveis