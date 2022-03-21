<h1 align="center"> Pipeline de Processamento para Dados Públicos de CNPJ </h1>

<p align="center">
<img src="https://user-images.githubusercontent.com/96552968/159188707-f9ec21c9-885d-42e0-9b1e-2f3bfc475123.png" width="750" height="500" >
</p>

<p align="center">
<img src="http://img.shields.io/static/v1?label=STATUS&message=EM%20DESENVOLVIMENTO&color=GREEN&style=for-the-badge"/>
</p>

## Índice 

* [Índice](#índice)
* [Descrição do Projeto](#descrição-do-projeto)
* [Funcionalidades e Demonstração da Aplicação](#funcionalidades-e-demonstração-da-aplicação)
* [Acesso ao Projeto](#acesso-ao-projeto)
* [Tecnologias utilizadas](#tecnologias-utilizadas)
* [Pessoa Desenvolvedora do Projeto](#pessoas-desenvolvedoras)


## Descrição do Projeto

   Neste projeto, foi construído um pipeline em python que realiza o download, processamento e inserção de um conjunto de dados em um Database (MySQL) automaticamente. Os dados são extraídos de um site do governo federal (https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj). São dados de `empresas`, `estabelecimentos` e `sócios`. Podemos observar, na próxima imagem, a estrutura deste projeto! 
  
<p align="center">
<img src="https://user-images.githubusercontent.com/96552968/159190254-287f4918-8c81-4f14-83a0-f104f85d6917.png" width="550" height="300" >
</p>

### Descrição das etapas do projeto: 
  
  - `RAW`: Nesta etapa é realizado o download de arquivos brutos do servidor do governo federal. Estes, são salvos em uma pasta chamada /Raw, que o próprio pipeline cria. 
  - `Standardized`: Na segunda etapa, os arquivos são descompactados e salvados como .csv em uma outra pasta que o próprio pipeline cria, chamada /Standardized.
  - `Conformed`: Na terceira etapa, os dados são carregados em objetos DataFrame (pandas) e processados. Os dados são padronizados em text e datetime. Além disso, as colunas das tabelas são nomeadas.    
  - `Aplicação`: Na última etapa, os dados processados são inseridos em um banco de dados (MySQL) 
  
## Funcionalidades e Demonstração da Aplicação
   Após rodar o programa completo, podemos observar as tabelas no Database selecionado pelo usuário. Logo, nossa aplicação se encontra funcional.   
<p align="center">
<img src="https://user-images.githubusercontent.com/96552968/159191321-304bcc6d-f4a7-4920-8526-e03e9ee068ba.png" >
</p>

## Acesso ao Projeto
### Dicas para rodar o projeto: 
   - Abrir o cmd, navegar até uma pasta local de projetos:
      - `mkdir projeto-pipeline-Cnpj`
      - `cd projeto-pipeline-Cnpj`
   - Se for o caso, crie um ambiente virtual:
      - `python -m venv base`
      - `base\Scripts\activate.bat`
   - Clone o repositório: 
      - `git clone https://github.com/zeolacode/Pipeline-de-Processamento-para-Dados-Publicos-de-CNPJ.git`
      - `cd Pipeline-de-Processamento-para-Dados-Publicos-de-CNPJ`
   - Pacotes necessários para rodar o código (instalar!):
      - `pip install tqdm`
      - `pip install requests`
      - `pip install pandas`
      - `pip install sqlalchemy`
      - `pip install PyMySQL`
   - Obs: Não esquecer de modificar as configurações da conexão com o MySQL no arquivo `pipeline.py` (também criar o database)!!
      - Nome das variáveis para modificação:
      - `database`
      - `usuario_db`
      - `senha_db`
   - Obs: A base de dados disponibilizada pelo governo, está particionada em 10 blocos! Você pode modificar o número de partições para que o pipeline realize o          processo com mais dados (por default o pipeline está realizando o processo, somente com a primeira partição)! Basta modificar as variáveis para relizar o download de mais partições `1 -> 10`:
      - `url_empresa`
      - `url_estabelecimento`
      - `url_socio`
   - Rodando a aplicação:  
      - `python pipeline.py`

## Tecnologias utilizadas
   - `Python`
   - `MySQL`

## Pessoa Desenvolvedora do Projeto
   - Pedro Zeola Lopes
 
