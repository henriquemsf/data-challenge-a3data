from ftplib import FTP
import pandas as pd
import py7zr
import zipfile
import os
import psycopg2

folder = r'C:\Users\rique\Nova pasta'

anos = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]

estabelecimentos = ['ESTB', 'Estb', 'ESTAB']

def extractData(anos):

    for ano in anos:

        #Conectar na base de dados e baixar os arquivos necessarios 
    
        ftp = FTP('ftp.mtps.gov.br')

        ftp.login()

        ftp.cwd('pdet/microdados')

        ftp.cwd('RAIS')

        ftp.cwd(str(ano))

        files = ftp.nlst()

        #Removemos arquivos que contenham as strings descritas na lista "estabelecimentos" pois nesse caso estaremos utilizando apenas os dados relacionados a vinculos
        
        for file in files:
            if any(estabelecimentos in file for estabelecimentos in estabelecimentos):
                files.remove(file)

        for file in files:
            with open(os.path.join(folder, str(ano), file), 'wb') as archive:
                ftp.retrbinary('RETR ' + file, archive.write)

def transformData(anos):

    #Criação de um dataFrame para consolidar as informações das regiões/estados
    
    dataFrame = pd.DataFrame({'CNAE 2.0 Classe': pd.Series([], dtype='int32'),
       'Escolaridade': pd.Series([], dtype='int8'),
       'Qtd Hora': pd.Series([], dtype='int8'),
       'Município': pd.Series([], dtype='int32'),
       'Remuneracao Media': pd.Series([], dtype='float32'),
       'Sexo': pd.Series([], dtype='int8'),
        'Trab Intermitente' : pd.Series([], dtype='int8'),
       'Ano': pd.Series([], dtype='int16')
      })

    for ano in anos:

        #Descompactar os arquivo baixados anteriormente
        
        for file in os.listdir(os.path.join(folder, str(ano))):
            with py7zr.SevenZipFile(os.path.join(folder, str(ano), file), 'r') as archive:
                archive.extractall(path=os.path.join(folder, str(ano)))                     
            os.remove(os.path.join(folder, str(ano), file))
        
        for filename in os.listdir(os.path.join(folder, str(ano))):

            if ano < 2017:

                #Criação de um dataframe provisório para cada arquivo
                #Importante: utilizar o datatype correto para as colunas diminui de forma significativa o consumo de recursos do programa

                df = pd.read_csv(os.path.join(folder, str(ano), filename), delimiter = ';', encoding = 'latin-1',
                        usecols =['CNAE 2.0 Classe', 'Escolaridade após 2005', 'Qtd Hora Contr', 'Município', 'Vl Remun Média (SM)', 'Sexo Trabalhador'],
                        dtype = {'CNAE 2.0 Classe' : 'int32' , 'Escolaridade após 2005' : 'int8', 'Qtd Hora Contr' : 'int8', 'Município' : 'int32', 'Vl Remun Média (SM)' : 'float32', 'Sexo Trabalhador' : 'int8'},
                                decimal=',')
                
                #Renomear as colunas para controle

                df.columns = ['CNAE 2.0 Classe','Escolaridade', 'Qtd Hora', 'Município', 'Remuneracao Media', 'Sexo']

                #Anos anteriores a 2017 não possuem informação de trabalho intermitente, devido a isso aplicamos o valor 0 aos mesmos

                df['Trab Intermitente'] = 0

                df['Ano'] = ano

                df = df.astype({'Ano' : 'int16', 'Trab Intermitente' : 'int8'})

            else:

                df = pd.read_csv(os.path.join(folder, str(ano), filename), delimiter = ';', encoding = 'latin-1',
                    usecols =['CNAE 2.0 Classe', 'Escolaridade após 2005', 'Qtd Hora Contr', 'Município', 'Vl Remun Média (SM)', 'Sexo Trabalhador', 'Ind Trab Intermitente'],
                    dtype = {'CNAE 2.0 Classe' : 'int32' , 'Escolaridade após 2005' : 'int8', 'Qtd Hora Contr' : 'int8', 'Município' : 'int32', 'Vl Remun Média (SM)' : 'float32', 'Sexo Trabalhador' : 'int8', 'Ind Trab Intermitente' : 'int8'},
                            decimal=',')

                df.columns = ['CNAE 2.0 Classe','Escolaridade', 'Qtd Hora', 'Município', 'Remuneracao Media', 'Sexo', 'Trab Intermitente']

                df['Ano'] = ano

                df = df.astype({'Ano' : 'int16'})

            #Adicionar o dataframe para o dataframe consolidado

            dataFrame = dataFrame.append(df, ignore_index=True)

            df = df.iloc[0:0]

            #Remover o arquivo utilizado

            os.remove(os.path.join(os.path.join(folder, str(ano), filename)))
                                
        csvName = str(ano) + 'dataframe.csv'
        
        dataFrame.info()

        #Exportar o dataframe consolidado para o formato csv

        dataFrame.to_csv(os.path.join(folder, str(ano), csvName), index=False)
        
        dataFrame = pd.DataFrame({'CNAE 2.0 Classe': pd.Series([], dtype='int32'),
        'Escolaridade': pd.Series([], dtype='int8'),
        'Qtd Hora': pd.Series([], dtype='int8'),
        'Município': pd.Series([], dtype='int32'),
        'Remuneracao Media': pd.Series([], dtype='float32'),
        'Sexo': pd.Series([], dtype='int8'),
            'Trab Intermitente' : pd.Series([], dtype='int8'),
        'Ano': pd.Series([], dtype='int16')
        })

def loadData(anos):

    #Conectar na base de dados (local, nesse caso)
    conn = psycopg2.connect('postgresql://postgres:admin@localhost:5432/RAIS')

    cur = conn.cursor()

    #Query que realiza a copia das informações disponibilizadas no cvs para a tabela desejada
    copy_sql = """
            COPY "RAIS" FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

    #Realiza a query para todos os arquivos disponíveis
    for ano in anos:
        csvName = str(ano) + "dataframe.csv"
        with open(os.path.join(folder, str(ano), csvName), 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
