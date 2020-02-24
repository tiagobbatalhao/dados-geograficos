import requests
import os
import pandas
import json

class API_IBGE_Malhas:

    url = 'http://servicodados.ibge.gov.br/api/v2/malhas'

    @classmethod
    def get(cls, id_, divisoes=None):
        resolucao_dict = {
            'macrorregioes': 1,
            'uf': 2,
            'mesorregioes': 3,
            'microrregioes': 4,
            'municipios': 5,
        }
        payload = {
            'formato': 'application/vnd.geo+json',
            'resolucao': resolucao_dict.get(divisoes, 0)
        }
        payload_str = '&'.join('{}={}'.format(k,v) for k,v in payload.items())
        req = requests.get(os.path.join(cls.url, str(id_)), params=payload_str)
        print(req.url)
        return req.json()


class API_IBGE_Municipios:

    url = 'http://servicodados.ibge.gov.br/api/v1/localidades'

    @classmethod
    def get(cls):
        payload = {}
        payload_str = '&'.join('{}={}'.format(k,v) for k,v in payload.items())        
        req = requests.get(os.path.join(cls.url, 'municipios'), params=payload_str)
        return req.json()


def parse_municipios_to_df(municipios):
    df = pandas.DataFrame(municipios).rename(columns={
        'id': 'municipio_id',
        'nome': 'municipio_nome',
    })

    levels = ['microrregiao', 'mesorregiao', 'UF', 'regiao']

    level_this = levels[0]
    level_next = levels[1]
    df[level_this.lower() + '_id'] = df[level_this].apply(lambda x: x['id'])
    df[level_this.lower() + '_nome'] = df[level_this].apply(lambda x: x['nome'])
    df[level_next] = df[level_this].apply(lambda x: x[level_next])
    df = df.drop(level_this, axis=1)

    level_this = levels[1]
    level_next = levels[2]
    df[level_this.lower() + '_id'] = df[level_this].apply(lambda x: x['id'])
    df[level_this.lower() + '_nome'] = df[level_this].apply(lambda x: x['nome'])
    df[level_next] = df[level_this].apply(lambda x: x[level_next])
    df = df.drop(level_this, axis=1)

    level_this = levels[2]
    level_next = levels[3]
    df[level_this.lower() + '_id'] = df[level_this].apply(lambda x: x['id'])
    df[level_this.lower() + '_nome'] = df[level_this].apply(lambda x: x['nome'])
    df[level_this.lower() + '_sigla'] = df[level_this].apply(lambda x: x['sigla'])
    df[level_next] = df[level_this].apply(lambda x: x[level_next])
    df = df.drop(level_this, axis=1)

    level_this = levels[3]
    df[level_this.lower() + '_id'] = df[level_this].apply(lambda x: x['id'])
    df[level_this.lower() + '_nome'] = df[level_this].apply(lambda x: x['nome'])
    df[level_this.lower() + '_sigla'] = df[level_this].apply(lambda x: x['sigla'])
    df = df.drop(level_this, axis=1)

    return df

def main():

    folder_save = os.path.join(
        os.path.expanduser('~/localdatalake'),
        'geograficos',
        'ibge',
    )
    municipios = API_IBGE_Municipios.get()
    municipios_df = parse_municipios_to_df(municipios)
    municipios_df.to_csv(
        os.path.join(folder_save, 'IBGE_municipios.csv'),
        sep=';', header=True, index=False,
    )

    ids = set()
    for column in municipios_df.columns:
        if column.endswith('_id'):
            for id_ in municipios_df[column]:
                ids.add(str(id_))
    ids.add('BR')
    levels = [
        'macrorregioes',
        'uf',
        'mesorregioes',
        'microrregioes',
        'municipios',
        'self',
    ]

    filename = os.path.join(folder_save, 'IBGE_malhas.json')
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            malhas = json.load(f)
    else:
        malhas = {}
    try:
        for id_ in sorted(ids):
            for level in levels:
                try:
                    key = '{}-{}'.format(id_, level)
                    if key not in malhas:
                        malhas[key]   = API_IBGE_Malhas.get(id_, level)
                except (requests.exceptions.ConnectionError):
                    continue
    except (KeyboardInterrupt, ):
        pass
    with open(filename, 'w') as f:
        json.dump(malhas, f)

    return municipios_df, malhas

if __name__ == '__main__':
    main()