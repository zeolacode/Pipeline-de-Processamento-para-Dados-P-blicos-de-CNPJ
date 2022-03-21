# Pacotes importados
from tqdm import tqdm
import requests
import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd

#########################################################
# Pipeline de Processamento para Dados Públicos de CNPJ #
#########################################################

# Criando diretórios 
cwd = os.getcwd()
print(f'Main directory: {cwd}')

if not os.path.exists('Raw'):
    os.makedirs('Raw')

if not os.path.exists('Standardized'):
    os.makedirs('Standardized')

path_raw = cwd + '/Raw/'
path_standardized = cwd + '/Standardized/'

# MySQL
database = 'cnpj_roit'
usuario_db = 'root'
senha_db = 'zeola123'

eng_mysql = f'mysql+pymysql://{usuario_db}:{senha_db}@localhost/{database}'
engine = create_engine(eng_mysql)

# Layout da base de dados
empresa = list({'cnpj_basico': str, 
                'razao_social': str, 
                'natureza_juridica': str, 
                'qualificacao_responsavel': str,
                'capital_social': str, 
                'porte_empresa': str, 
                'ente_federativo': str })

estabelecimento = list({'cnpj_basico': str, 
                        'cnpj_ordem': str, 
                        'cnpj_dv': str, 
                        'id_matriz_filial': str, 
                        'nome_fantasia': str, 
                        'situacao_cadastral': str,
                        'data_situacao_cadastral': str, 
                        'motivo_situacao_cadastral': str, 
                        'cidade_exterior': str, 
                        'pais': str, 
                        'data_inicio_atividade': str,
                        'cnae_principal': str, 
                        'cnae_secundario': str, 
                        'tipo_logradouro': str, 
                        'logradouro': str, 
                        'numero': str, 
                        'complemento': str,
                        'bairro': str, 
                        'cep': str, 
                        'uf': str, 
                        'municipio': str, 
                        'ddd1': str, 
                        'telefone1': str, 
                        'ddd2': str, 
                        'telefone2': str,
                        'ddd_fax': str, 
                        'fax': str, 
                        'email': str, 
                        'situacao_especial': str, 
                        'data_situacao_especial': str })

socio = list({'cnpj_basico': str, 
              'id_socio': str, 
              'nome': str, 
              'cpf_cnpj': str, 
              'qualificacao': str, 
              'data_entrada': str,
              'pais': str, 
              'representante': str, 
              'nome_representante': str, 
              'qualificacao_representante': str, 
              'faixa_etaria': str })


########### 
# Funções #
###########

# Download 
def download_arquivo(url, endereco):
  chunk_size = 1024
  r = requests.get(url, stream = True)
  total_size = int(r.headers['content-length'])
  
  filename = url.split('/')[-1]
  path = endereco + filename
  
  with open(path, 'wb') as f:
	  for data in tqdm(iterable = r.iter_content(chunk_size = chunk_size), total = total_size/chunk_size, unit = 'KB'):
		  f.write(data)

# lista os arquivos de um diretório
def list_paht(mypath):
  list_paht = []
  for (dirpath, dirnames, filenames) in os.walk(mypath):
      list_paht.extend(filenames)
      break
  return list_paht

# Unzip
def unzip():
  x = list_paht(path_raw)
  for i in x:
    cwd_file = path_raw + i 
    fantasy_zip = zipfile.ZipFile(cwd_file)
    fantasy_zip.extractall(path_standardized)
    fantasy_zip.close()

# Transforma dados em .csv
def transforming_to_csv():
  s = list_paht(path_standardized)
  for i in s:
    cwd_file = path_standardized + i
    base_f = os.path.splitext(cwd_file)
    base = os.path.splitext(cwd_file)[0]
    if(base_f[1] == '.EMPRECSV'):
      os.rename(cwd_file, base + '.EMPRE.csv')
    elif(base_f[1] == '.ESTABELE'):
      os.rename(cwd_file, base + '.ESTABELE.csv')
    elif(base_f[1] == '.SOCIOCSV'):
      os.rename(cwd_file, base + '.SOCIO.csv')

# Processamento dos dados e envio para o banco de dados MySQL
def processing_to_mysql():
  # Lista arquivos do diretório 
  w = list_paht(path_standardized)
  for i in w:
    cwd_file = path_standardized + i
    if 'EMPRE.csv' in cwd_file:
      # Leitura dos arquivos em formato .csv com pandas 
      print(cwd_file + ' --> MySQL')
      chunk = pd.read_csv(cwd_file, delimiter=';', header=None, chunksize=50000, names=empresa, iterator=True, dtype=str, encoding="ISO-8859-1")
      df = pd.concat(chunk)
      
      # Incerindo dados no MySQL
      df.to_sql('empresas', engine, if_exists='append', index=False, chunksize=50000)
    
    elif 'ESTABELE.csv' in cwd_file:
      # Leitura dos arquivos em formato .csv com pandas
      print(cwd_file + ' --> MySQL') 
      chunk = pd.read_csv(cwd_file, delimiter=';', header=None, chunksize=50000, names=estabelecimento, iterator=True, dtype=str, encoding="ISO-8859-1")
      df = pd.concat(chunk)
      
      # Transformando dados no tipo Data
      df.loc[df['data_situacao_cadastral'] == '00000000', 'data_situacao_cadastral'] = None
      df.loc[df['data_situacao_cadastral'] == '0', 'data_situacao_cadastral'] = None
      df['data_situacao_cadastral'] = pd.to_datetime(df['data_situacao_cadastral'], format='%Y%m%d', errors='coerce').dt.date

      df.loc[df['data_inicio_atividade'] == '00000000', 'data_inicio_atividade'] = None
      df.loc[df['data_inicio_atividade'] == '0', 'data_inicio_atividade'] = None
      df['data_inicio_atividade'] = pd.to_datetime(df['data_inicio_atividade'], format='%Y%m%d', errors='coerce').dt.date 

      df.loc[df['data_situacao_especial'] == '00000000', 'data_situacao_especial'] = None
      df.loc[df['data_situacao_especial'] == '0', 'data_situacao_especial'] = None
      df['data_situacao_especial'] = pd.to_datetime(df['data_situacao_especial'], format='%Y%m%d', errors='coerce').dt.date
      
      # Incerindo dados no MySQL
      df.to_sql('estabelecimentos', engine, if_exists='append', index=False, chunksize=50000)

    elif 'SOCIO.csv' in cwd_file:
      # Leitura dos arquivos em formato .csv com pandas 
      print(cwd_file + ' --> MySQL')
      chunk = pd.read_csv(cwd_file, delimiter=';', header=None, chunksize=50000, names=socio, iterator=True, dtype=str, encoding="ISO-8859-1")
      df = pd.concat(chunk)

      # Transformando dados no tipo Data
      df.loc[df['data_entrada'] == '00000000', 'data_entrada'] = None
      df.loc[df['data_entrada'] == '0', 'data_entrada'] = None
      df['data_entrada'] = pd.to_datetime(df['data_entrada'], format='%Y%m%d', errors='coerce').dt.date

      # Incerindo dados no MySQL
      df.to_sql('socios', engine, if_exists='append', index=False, chunksize=50000)

#################################################
# RAW -> Standardized -> Conformed -> Aplicação #
#################################################

#######
# RAW #
#######

# Limpar arquivos antigos da pasta Raw
# dir = path_raw
# for f in os.listdir(dir):
#     os.remove(os.path.join(dir, f))

# Download de arquivos brutos na pasta Raw
print("Starting the download!")

# EMPRESA
url_empresa = 1                        # número de partes que deseja realizar o download (1 até 10)
for i in range(url_empresa):
  link_empresa = f'http://200.152.38.155/CNPJ/K3241.K03200Y{i}.D20212.EMPRECSV.zip'
  download_arquivo(link_empresa, path_raw)

# ESTABELECIMENTO
url_estabelecimento = 1                # número de partes que deseja realizar o download (1 até 10)
for i in range(url_estabelecimento):
  link_estabelecimento = f'http://200.152.38.155/CNPJ/K3241.K03200Y{i}.D20212.ESTABELE.zip'
  download_arquivo(link_estabelecimento, path_raw) 

# SÓCIO
url_socio = 1                          # número de partes que deseja realizar o download (1 até 10)
for i in range(url_socio):
  link_socio = f'http://200.152.38.155/CNPJ/K3241.K03200Y{i}.D20212.SOCIOCSV.zip'
  download_arquivo(link_socio, path_raw)

print("Download complete!")


################
# Standardized #
################

# Limpar arquivos antigos da pasta Raw
# dir = path_standardized
# for f in os.listdir(dir):
#   os.remove(os.path.join(dir, f))

# Descompactando arquivos brutos e salvando em Standardized
unzip()

# Transforma arquivos descompactados da pasta (Standardized) em .csv
transforming_to_csv()

print("Standardized complete!")

###########################
# Conformed and Aplicação #
###########################

# Processamento disponibilização dos dados no banco de dados MySQL
processing_to_mysql()

print("Conformed and Aplicação complete!")
print("Pipeline complete!")