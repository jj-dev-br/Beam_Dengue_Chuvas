import pandas
arquivo = '/home/indicium-leo/ProjetosPessoais/BeamTooltorial/resultado-00000-of-00001.csv'
df = pandas.read_csv(arquivo, delimiter=';')
print(df.groupby(['UF','ANO'])[['CHUVA','DENGUE']].mean().reset_index)