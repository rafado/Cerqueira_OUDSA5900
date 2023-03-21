#!/Library/Frameworks/Python.framework/Versions/3.9/bin/python3
# coding=utf-8
import pandas as pd
import sqlite3
import csv
import numpy as np
import requests
import time
import io
from datetime import date
from datetime import timedelta


def collect_csv(temp_url):
    # Response collects the file from the specified url. Have to add User Agent to the url in order to allow access.
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    response = requests.get(temp_url, headers=headers)

    # Collects data into a dataframe and delimits data before assigning feature names.
    temp_df = pd.read_csv(io.StringIO(response.content.decode('utf-8')), parse_dates=True)
    return temp_df

def clean_dataframe(temp_df, file_name, temp_file_index, temp_features):
    # Death, hospitalizations, and vaccinations data does not need to be delimited and requires different cleaning.
    if file_name != 'DHV_' + today_str + '.csv':
        # Delimits data when a semi-colon appears.
        temp_df.columns = ['Data']
        temp_df = temp_df['Data'].str.split(';', expand=True)

        # Inserts temp_features as column headers and then drops NA values. Also assigns fecha_resulatado as a datetime.
        temp_df.columns = [temp_features[temp_file_index]]
        temp_df = temp_df.replace(r'^\s*$', pd.NA, regex=True)
        temp_df = temp_df.dropna()
        temp_df['fecha_resultado'] = temp_df['fecha_resultado'].astype('datetime64[ns]')
    else:
        # Inserts date that the data was collected and assigns columns headers.
        temp_df.columns = [temp_features[temp_file_index]]
        temp_df.insert(1, 'fecha_recopilacion', time.strftime("%Y-%m-%d"))

        # Drops last four columns as they are redundant.
        temp_df = temp_df.iloc[:, :-4]

        # Converts dates exclusive to DHV into date data types.

    # Drops duplicate rows.
    temp_df.drop_duplicates(keep=False, inplace=True)

    # Converts attributes to desired data type which is universal to all data sets.
    temp_df['fecha_recopilacion'] = temp_df['fecha_recopilacion'].astype('datetime64[ns]')

    # Cleans Classification type for deaths since special characters appear in current data.
    if file_name == 'Deaths_' + today_str + '.csv' or file_name == 'DHV_' + today_str + '.csv':
        # Replacing values with special characters with more readable variations.
        temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio virolÃ³gico', 'Virological', temp_df.criterio_fallecido)
        temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio SINADEF', 'SINADEF', temp_df.criterio_fallecido)
        temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio serolÃ³gico', 'Serological', temp_df.criterio_fallecido)
        temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio investigaciÃ³n EpidemiolÃ³gica', 'Epidemiological investigation', temp_df.criterio_fallecido)
        temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio clÃ\xadnico', 'Clinical', temp_df.criterio_fallecido)
        temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio radiolÃ³gico', 'Radiological', temp_df.criterio_fallecido)
        temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio nexo epidemiolÃ³gico', 'Epidemiological link', temp_df.criterio_fallecido)
    return temp_df


def data_quality_reports(temp_df):
    # Creating a data quality report to analyze the data frame.
    data_types = pd.DataFrame(temp_df.dtypes, columns=['Data Type'])
    missing_data = pd.DataFrame(temp_df.isnull().sum(), columns=['Missing Values'])
    unique_values = pd.DataFrame(temp_df.nunique(), columns=['Unique Values'])
    minimum_values = pd.DataFrame(temp_df.min(), columns=['Minimum Value'])
    maximum_values = pd.DataFrame(temp_df.max(), columns=['Maximum Value'])

    dq_report = data_types.join(missing_data).join(unique_values).join(minimum_values).join(maximum_values)
    print(df)
    print(dq_report.to_string())

# List that store the URL of the data, desired file names, and the features associated with each data table.
urls = ['https://files.minsa.gob.pe/s/eRqxR35ZCxrzNgr/download', 'https://files.minsa.gob.pe/s/t9AFqRbXw3F55Ho/download', 'https://cloud.minsa.gob.pe/s/8EsmTzyiqmaySxk/download']
file_names = ['Positive_Cases_', 'Deaths_', 'DHV_']
features = [['fecha_recopilacion', 'departamento', 'provincia', 'distrito', 'metodo', 'edad', 'sexo', 'fecha_resultado', 'UBIGEO', 'UUID'],
            ['fecha_recopilacion', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'departamento', 'provincia', 'distrito', 'UBIGEO', 'UUID'],
            ['UUID', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'UBIGEO', 'departamento', 'provincia', 'distrito',
            'cdc_positividad', 'flag_vacuna', 'fecha_dosis1', 'fabricante_dosis1', 'fecha_dosis2',
            'fabricante_dosis2', 'fecha_dosis3', 'fabricante_dosis3', 'flag_hospitalizado', 'eess_renaes', 'eess_diresa',
            'eess_red', 'eess_nombre',	'fecha_ingreso_hosp', 'flag_uci', 'fecha_ingreso_uci', 'fecha_ingreso_ucin',
            'con_oxigeno', 'con_ventilacion', 'fecha_segumiento_hosp_ultimo', 'evolucion_hosp_ultimo', 'ubigeo_inei_domicilio', 'dep_domicilio', 'prov_domicilio', 'dist_domicilio']]

# Index for iterating through file name list/features and gets current time.
file_index = 0
# Get today's date
today = date.today()
today_str = str(today)

# Yesterday date
yesterday_str = str(today - timedelta(days = 1))

#timestr = time.strftime("%Y-%m-%d")
# Iterates through URL list and grabs specified data.
for url in urls:
    # Appends current date and file type to base file name.
    file_names[file_index] += today_str
    file_names[file_index] += ".csv"

    # Response collects the file from the specified url.
    print("Currently downloading", file_names[file_index], "from Peru's National Open Data Platform...")
    print("This might take awhile...")

    # Get CSV from specified url and places it into a dataframe.
    df = collect_csv(url)

    # Puts data through a definition to clean the data.
    df = clean_dataframe(df, file_names[file_index], file_index, features)

    # Generates a data quality report
    print("Generating a data quality report for", file_names[file_index])
    data_quality_reports(df)
    df

    # Saves the dataframe to a csv file in the current directory.
    df.to_csv(file_names[file_index], index=False)
    print(file_names[file_index], "filenames")

    # Prints file that was saved and changes the index.
    print(file_names[file_index], "has been successfully downloaded and cleaned!")
    print("")
    file_index += 1

# Read sqlite query results into a pandas DataFrame
con1 = sqlite3.connect("PeruData.db")
print(con1)

cursor1 = con1.cursor()

# Create table to store Death Data
cursor1.execute('''CREATE TABLE IF NOT EXISTS Deaths
                 (fecha_recopilacion TEXT NOT NULL,
                  fecha_resultado TEXT NOT NULL,
                  edad INTEGER,
                  sexo TEXT NOT NULL,
                  criterio_fallecido TEXT NOT NULL,
                  departamento TEXT NOT NULL,
                  provincia TEXT NOT NULL,
                  distrito TEXT NOT NULL,
                  UBIGEO INTEGER,
                  UUID INTEGER)''')
# Insert data into death table
feature = ['fecha_recopilacion', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'departamento', 'provincia', 'distrito', 'UBIGEO','UUID']
death_data = pd.read_csv(file_names[1])
death_data = death_data[feature]
death_data.to_sql('Deaths', con1, if_exists='append', index=False)

df_deaths = pd.read_sql_query("SELECT * from Deaths", con1)

# sort based on date acquired (fecha_resultado) feature.
df_deaths_sorted = df_deaths.sort_values(by='fecha_resultado')

df_deaths_sorted.drop('fecha_recopilacion', axis=1, inplace=True)

# Get today's date
today = date.today()
today_str = str(today)
file_name = 'Deaths_'+today_str+'.csv'
# print(file_name)
print(file_names[1])
df = pd.read_csv(file_names[1])


# sort based on date acquired (fecha_resultado) feature.
df = df.sort_values(by='fecha_resultado')
df.drop('fecha_recopilacion', axis=1, inplace=True)
print("database df rows: ", df_deaths_sorted.shape[0])
print(df_deaths_sorted.tail(5))
print("yesterday's df rows: ", df.shape[0])

if df.shape[0]>df_deaths_sorted.shape[0]:
    df3 = df[df_deaths_sorted.shape[0]:]
    df_deaths_sorted.append(df3, ignore_index = True)
    res = [df_deaths_sorted, df3]
    df_deaths_sorted = pd.concat(res)

# print(df_deaths_sorted.shape[0])
df_deaths_sorted = df_deaths_sorted.reset_index()
df = df.reset_index()
df.drop('index', axis=1, inplace=True)
df_deaths_sorted.drop('index', axis=1, inplace=True)


#print(df3)
print(df.tail(31))
print(df_deaths_sorted.tail(31))


# Positive Cases

# Create table to store Positive Cases Data
cursor1.execute('''CREATE TABLE IF NOT EXISTS Positive_Cases
                 (fecha_recopilacion TEXT NOT NULL,
                  departamento TEXT NOT NULL,
                  provincia TEXT NOT NULL,
                  distrito TEXT NOT NULL,
                  metodo TEXT NOT NULL,
                  edad INTEGER,
                  sexo TEXT NOT NULL,
                  criterio_fallecido TEXT,
                  fecha_resultado TEXT NOT NULL,
                  UBIGEO INTEGER,
                  UUID INTEGER)''')

# Insert data into cases table

#feature1 = ['fecha_recopilacion','departamento','provincia','distrito','metodo','edad','sexo','fecha_resultado','UBIGEO','UUID']
cases_data = pd.read_csv(file_names[0])
cases_data = cases_data.rename(columns={'departmento': 'departamento'})
cases_data = cases_data[features[0]]
cases_data.to_sql('Positive_Cases', con1, if_exists='append', index=False)

df_cases = pd.read_sql_query("SELECT * from Positive_Cases", con1)

# sort based on date acquired (fecha_resultado) feature.
df_cases_sorted = df_cases.sort_values(by='fecha_resultado')

df_cases_sorted.drop('fecha_recopilacion', axis=1, inplace=True)

# Get today's date
today1 = date.today()
today_str1 = str(today1)
file_name = 'Positive_Cases_'+today_str+'.csv'
# print(file_name)
print(file_names[0])
df1 = pd.read_csv(file_names[0])


# sort based on date acquired (fecha_resultado) feature.
df1 = df1.sort_values(by='fecha_resultado')
df1.drop('fecha_recopilacion', axis=1, inplace=True)
print("database df rows: ", df_cases_sorted.shape[0])
print(df_cases_sorted.tail(5))
print("yesterday's df rows: ", df1.shape[0])

if df1.shape[0]>df_cases_sorted.shape[0]:
    df3_1 = df1[df_cases_sorted.shape[0]:]
    df_cases_sorted.append(df3_1, ignore_index = True)
    res1 = [df_cases_sorted, df3_1]
    df_cases_sorted = pd.concat(res1)

# print(df_deaths_sorted.shape[0])
df_cases_sorted = df_cases_sorted.reset_index()
df1 = df1.reset_index()
df1.drop('index', axis=1, inplace=True)
df_cases_sorted.drop('index', axis=1, inplace=True)


# print(df3_1)
print(df1.tail(31))
print(df_cases_sorted.tail(31))


# Create table to store DHV Data
cursor1.execute('''CREATE TABLE IF NOT EXISTS DHV
                 (UUID INTEGER,
                  fecha_recopilacion TEXT,
                  fecha_resultado TEXT NOT NULL,
                  edad INTEGER,
                  sexo TEXT NOT NULL,
                  criterio_fallecido TEXT NOT NULL,
                  UBIGEO INTEGER,
                  departamento TEXT NOT NULL,
                  provincia TEXT NOT NULL,
                  distrito TEXT NOT NULL,
                  cdc_positividad INTEGER,
                  flag_vacuna INTEGER,
                  fecha_dosis1 TEXT,
                  fabricante_dosis1 TEXT,
                  fecha_dosis2 TEXT,
                  fabricante_dosis2 TEXT,
                  fecha_dosis3 TEXT,
                  fabricante_dosis3 TEXT,
                  flag_hospitalizado INTEGER,
                  eess_renaes INTEGER,
                  eess_diresa INTEGER,
                  eess_red TEXT,
                  eess_nombre TEXT,
                  fecha_ingreso_hosp TEXT,
                  flag_uci INTEGER,
                  fecha_ingreso_uci TEXT,
                  fecha_ingreso_ucin TEXT,
                  con_oxigeno INTEGER,
                  con_ventilacion INTEGER,
                  fecha_segumiento_hosp_ultimo TEXT,
                  evolucion_hosp_ultimo TEXT)''')
# Insert data into death table
feature2 = ['UUID', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'UBIGEO', 'departamento', 'provincia', 'distrito',
            'cdc_positividad', 'flag_vacuna', 'fecha_dosis1', 'fabricante_dosis1', 'fecha_dosis2',
            'fabricante_dosis2', 'fecha_dosis3', 'fabricante_dosis3', 'flag_hospitalizado', 'eess_renaes', 'eess_diresa',
            'eess_red', 'eess_nombre',	'fecha_ingreso_hosp', 'flag_uci', 'fecha_ingreso_uci', 'fecha_ingreso_ucin',
            'con_oxigeno', 'con_ventilacion', 'fecha_segumiento_hosp_ultimo', 'evolucion_hosp_ultimo']

DHV_data = pd.read_csv(file_names[2])
DHV_data = DHV_data[feature2]
DHV_data.to_sql('DHV', con1, if_exists='append', index=False)

df_DHV = pd.read_sql_query("SELECT * from DHV", con1)

# sort based on date acquired (fecha_resultado) feature.
df_DHV_sorted = df_DHV.sort_values(by='fecha_resultado')

df_DHV_sorted.drop('fecha_recopilacion', axis=1, inplace=True)

# Get today's date
today2 = date.today()
today_str2 = str(today2)
# file_name = 'DHV_'+today_str+'.csv'
# print(file_name)
print(file_names[2])
df2 = pd.read_csv(file_names[2])


# sort based on date acquired (fecha_resultado) feature.
df2 = df2.sort_values(by='fecha_resultado')
df2.drop('fecha_recopilacion', axis=1, inplace=True)
print("database df2 rows: ", df_DHV_sorted.shape[0])
print(df_DHV_sorted.tail(5))
print("yesterday's df2 rows: ", df2.shape[0])

if df2.shape[0]>df_DHV_sorted.shape[0]:
    df4 = df2[df_DHV_sorted.shape[0]:]
    #df_DHV_sorted.append(df4, ignore_index = True)
    res2 = [df_DHV_sorted, df4]
    df_DHV_sorted = pd.concat(res2)

# print(df_DHV_sorted.shape[0])
df_DHV_sorted = df_DHV_sorted.reset_index()
df2 = df2.reset_index()
df2.drop('index', axis=1, inplace=True)
df_DHV_sorted.drop('index', axis=1, inplace=True)


print(df2)
print(df2.tail(31))
print(df_DHV_sorted.tail(31))


# with open('myfile.csv', newline='') as csvfile:
#     reader = csv.reader(csvfile, delimiter=',')
#     for row in reader:
#         cursor.execute("INSERT INTO mytable (column1, column2, column3) VALUES (?, ?, ?)", row)

con1.commit()

con1.close()

