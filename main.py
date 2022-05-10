from email import header
from xml.dom.expatbuilder import ElementInfo
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re 


pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue =['id','data_iniSE','casos','ibge_code','cidade',
'uf','cep','latitude','longitude']

colunas_chuva=["data","mm","uf"]

def texto_para_lista(elemento, delimitador="|"):
    """
    Recebe um texto e um delimitador e retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def lista_para_dicionario(elemento, colunas):
    """""
    Recebe uma lista e transforma num dicionario
    """""
    return dict(zip(colunas,elemento))

def trata_datas(elemento):
    """"
    Recebe um dicionário e cria um campo novo ANO-MES E retorna o mesmo dicionario com o novo campo
    """
    dia_mes_ano = elemento['data_iniSE'].split('-')
    mes_ano = dia_mes_ano[:2]
    elemento['ano_mes'] = '-'.join(mes_ano)
    return elemento

def arredonda(elemento):
    """
    Recebe uma tupla ('PA-2019-06', 2364.000000000003)
    Retorna uma tupla com o valor arredondado ('PA-2019-06', 2364.0)
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def chave_uf(elemento):
    """
    Receber um dicionário e retornar uma tupla(UF, dicionário)
    """
    chave = elemento['uf']
    return (chave, elemento)

def chave_uf_ano_mes(elemento):
    """
    Receber uma lista de elementos
    Retornar uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-MES', 1.3)
    ['2016-01-24', '4.2', 'TO']
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

def casos_dengue(elemento):

    """
    Recebe uma tupla ('Estado', [dict])
    Retorna uma tupla ('RS-DATA', Qtd)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)
def filtra_campos_vazios(elemento):
    """
    Remove os elementos com chaves vazias na combinação das tuplas
    """
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    else:
        return False

def descompactar_elementos(elemento):
    """"
    Recebe uma tupla com as chaves e retorna só os valores
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, (ano), (mes), str(chuva), str(dengue)

def preparar_csv(elemento):
    """"
    Recebe uma tupla e retorna uma string delimitada por ;

    """
    return ";".join(elemento)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> 
        beam.Map(texto_para_lista)
    | "De lista para dicionário" >> 
        beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> 
        beam.Map(trata_datas)
    | "Criar chave pelo estado" >>  
        beam.Map(chave_uf)
    | "Agrupar por estado" >> 
        beam.GroupByKey()
    | "Descompactar casos de dengue" >> 
        beam.FlatMap(casos_dengue)
    | "Somar as ocorrencias de dengue" >> 
        beam.CombinePerKey(sum)
    # | "Mostrar resultados dengue" >> 
    #     beam.Map(print)
)

chuva = (
    pipeline
    | "Leitura do dataset de chuvas" >>
        ReadFromText('chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuvas)" >> 
        beam.Map(texto_para_lista, delimitador=',')
    | "Criando a chave UF-ANO-MES" >> 
        beam.Map(chave_uf_ano_mes)
    | "Somar as ocorrencias de chuva" >> 
         beam.CombinePerKey(sum)
    | "Arrendondar resultados de chuvas" >> 
        beam.Map(arredonda)
    # | "Mostrar resultados chuva" >> 
    #     beam.Map(print)
)

resultado = ( 
    ({'chuvas': chuva, 'dengue': dengue})
    | "Juntar e agrupar pcolections" >>
        beam.CoGroupByKey()
    | "Remover campos vazios" >>
        beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos" >>
        beam.Map(descompactar_elementos)
    | "Preparar csv" >>
        beam.Map(preparar_csv)

)
header = 'UF;ANO;MES;CHUVA;DENGUE'
resultado | 'Criar arquivo CSV' >> WriteToText('resultado', file_name_suffix='.csv', header=header)

pipeline.run()