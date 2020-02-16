import requests
import os
import pandas
import json
import io
import zipfile

class API_Censo_Setores:

    url = os.path.join(
        'http://geoftp.ibge.gov.br/',
        'organizacao_do_territorio',
        'malhas_territoriais',
        'malhas_de_setores_censitarios__divisoes_intramunicipais',
        'censo_2010',
        'setores_censitarios_shp',
        '{uf}/{uf}_{level}.zip', 
    )

    @classmethod
    def get(cls, uf, level):
        req = requests.get(cls.url.format(uf=uf.lower(), level=level.lower()), params='')
        print(req.url)
        content = io.BytesIO(req.content)
        try:
            zipped = zipfile.ZipFile(content)
            files = {x: zipped.open(x) for x in zipped.namelist()}
            return files
        except zipfile.BadZipFile:
            return {}



def main():

    folder_save = os.path.join(
        os.path.expanduser('~/localdatalake'),
        'geograficos',
        'setores_shp',
    )

    ufs = [
        # 'ap', 'am', 'ac', 'pa', 'ro', 'rr', 'to',
        # 'ba', 'se', 'al', 'pe', 'rn', 'pb', 'ce', 'pi', 'ma',
        'go', 'mt', 'ms', 'df',
        'sp', 'rj', 'es', 'mg',
        'rs', 'sc', 'pr',
    ]
    levels = [
        'distritos',
        'municipios',
        'setores_censitarios',
        'subdistritos',
    ]
    for uf in ufs:
        for level in levels:
            fls = API_Censo_Setores.get(uf, level)
            folder_this = os.path.join(folder_save, '{uf}_{level}'.format(uf=uf, level=level))
            if not os.path.exists(folder_this):
                os.makedirs(folder_this)
            for name, fl in fls.items():
                fname = os.path.join(folder_this, name)
                with open(fname, 'wb') as flsave:
                    flsave.write(fl.read())


if __name__ == '__main__':
    main()