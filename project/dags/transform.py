import pandas as pd

from airflow.decorators import task

@task
def transform(df):
    """ Transform data for preparing loading.

    Args:
        df (DataFrame): extracted data
    
    """

    print('Start transforming data...')

    # Lowercase for features
    df.columns = df.columns.map(lambda x: x.lower())

    # Remove columns/rows with missing values
    df.drop(columns=['numero', 'variete_ou_cultivar', 'typeemplacement', 'remarquable', 'pepiniere'], 
            axis=1, inplace=True)
    df.dropna(inplace=True)


    # Create two new DataFrames
    trees = df[['idbase', 'libellefrancais', 'genre', 
               'espece', 'circonference_en_cm', 'hauteur_en_m', 
               'stadedeveloppement', 'dateplantation', 'geo_point']]
    trees = trees.astype({
        'idbase': 'float64', 
        'libellefrancais': 'object', 
        'genre': 'object',  
        'espece': 'object', 
        'circonference_en_cm': 'float64', 
        'hauteur_en_m': 'float64', 
        'stadedeveloppement': 'object', 
        'dateplantation': 'object', 
        'geo_point': 'object'
    })
    trees['idbase'] = trees['idbase'].apply(lambda x: int(x))
    trees['dateplantation'] = pd.to_datetime(trees['dateplantation'])
    addresses = df[['adresse', 'domanialite', 'arrondissement', 
                   'complementadresse', 'idemplacement']]
    addresses = addresses.astype({
        'adresse': 'object',
        'domanialite': 'object', 
        'arrondissement': 'object', 
        'complementadresse': 'object', 
        'idemplacement': 'object'
    })
    
    print('Data successfully transformed!')

    return [addresses, trees]
