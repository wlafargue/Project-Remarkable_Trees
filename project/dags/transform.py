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
    df.drop(columns=['numero', 'variete_ou_cultivar', 'typeemplacement', 'remarquable', 'pepiniere'], inplace=True)
    df.dropna(inplace=True)

    # Create two new DataFrames
    trees = df[['idbase', 'libellefrancais', 'genre', 
               'espece', 'circonference_en_cm', 'hauteur_en_m', 
               'stadedeveloppement', 'dateplantation', 'geo_point']]
    adresses = df[['adresse', 'domanialite', 'arrondissement', 
                   'complementadresse', 'idemplacement']]
    
    print('Data successfully transformed!')

    return [adresses, trees]
