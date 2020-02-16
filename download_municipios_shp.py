import requests
import os
import pandas
import json
import io
import zipfile

class API_Censo_Brasil:

    url = os.path.join(
        'http://geoftp.ibge.gov.br/',
        'organizacao_do_territorio',
        'malhas_territoriais',
        'malhas_municipais',
        'municipio_2018',
        'Brasil',
        'BR',
    )

    @classmethod
    def get(cls, level):
        if level == 'all':
            url = os.path.join(cls.url, 'BR.zip')
        else:
            url = os.path.join(cls.url, 'br_{level}.zip').format(level=level.lower())
        req = requests.get(url, params='')
        print(req.url)
        content = io.BytesIO(req.content)
        zipped = zipfile.ZipFile(content)
        files = {x: zipped.open(x) for x in zipped.namelist()}
        return files



def main():

    folder_save = os.path.join(
        os.path.expanduser('~/localdatalake'),
        'geograficos',
        'municipios_shp',
    )

    levels = [
        # 'mesorregioes',
        # 'microrregioes',
        # 'municipios',
        # 'unidades_da_federacao',
        'all',
    ]
    for level in levels:
        fls = API_Censo_Brasil.get(level)
        for name, fl in fls.items():
            fname = os.path.join(folder_save, '{level}'.format(level=level), name)
            folder_this = os.path.dirname(fname)
            if not os.path.exists(folder_this):
                os.makedirs(folder_this)
            with open(fname, 'wb') as flsave:

                flsave.write(fl.read())


if __name__ == '__main__':
    main()