#!/Library/Frameworks/Python.framework/Versions/3.9/bin/python3
#!/usr/bin/python3
# coding=utf-8
# Python Script for Webscraping Peru's National Database, Cleaning Data, and Uploading it into a database.
# Created by Boyer Simpkins 09/13/2022 modified last on

# Possible Improvements
# 1. As of right now dates are not consistent. It would be nice to have them all in MM/DD/YYYY format.
# 2. Make defintions for each process such that this script could be called by other scripts.
# 3. Adding a progress bar to see how much longer each file has to download.

# Request allows for the extraction of data from a specified url.
# Pandas allows for data manipulation.
# Numpy allows for further manipulation.
# Time and io are built in Python libraries.
import numpy as np
import requests
import pandas as pd
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
features = [['fecha_recopilacion', 'departmento', 'provincia', 'distrito', 'metodo', 'edad', 'sexo', 'fecha_resultado', 'UBIGEO', 'UUID'],
            ['fecha_recopilacion', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'departamento', 'provincia', 'distrito', 'UBIGEO', 'UUID'],
            ['UUID', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'UBIGEO', 'departamento', 'provincia', 'distrito',
            'cdc_positividad', 'flag_vacuna', 'fecha_dosis1', 'fabricante_dosis1', 'fecha_dosis2',
            'fabricante_dosis2', 'fecha_dosis3', 'fabricante_dosis3', 'flag_hospitalizado', 'eess_renaes', 'eess_diresa',
            'eess_red', 'eess_nombre',	'fecha_ingreso_hosp', 'flag_uci', 'fecha_ingreso_uci', 'fecha_ingreso_ucin',
            'con_oxigeno', 'con_ventilacion', 'fecha_segumiento_hosp_ultimo', 'evolucion_hosp_ultimo', 'ubigeo_inei_domicilio',
            'dep_domicilio', 'prov_domicilio', 'dist_domicilio']]

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
