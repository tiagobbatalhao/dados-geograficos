import os
import urllib.request
import zipfile
import io
import pandas
import geopandas
import glob
import argparse

import sys
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            '..',
        )
    )
)
import connections

class FTP_CensoMicrodados:
    """
    Read list of district codes
    """
    url = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados/'

    ufs = [
        'ba', 'se', 'al', 'pe', 'rn', 'pb', 'ce', 'pi', 'ma',
        'go', 'mt', 'ms', 'df',
        'sp1', 'sp2_rm', 'rj', 'es', 'mg',
        'rs', 'sc', 'pr',
        'ap', 'am', 'ac', 'pa', 'ro', 'rr', 'to',
    ]

    @classmethod
    def get(cls, uf):
        url = os.path.join(cls.url, uf.upper()+'.zip')        
        with urllib.request.urlopen(url) as req:
            content = req.read()
        try:
            zipped = zipfile.ZipFile(io.BytesIO(content))
            files = {x: zipped.open(x) for x in zipped.namelist()}
            return files
        except (zipfile.BadZipFile, NotImplementedError):
            fname = uf.upper()+'.zip'
            print('Could not unzip:', fname)
            return {fname: io.BytesIO(content)}

    @classmethod
    def save(cls, files_dict):
        folder_save = os.path.join(
            'gs://tb-dados_geograficos',
            'raw',
            'censo_microdados',
        )
        gcs = connections.get_GCS_client()
        for name, fl in files_dict.items():
            if not name.endswith('/'):
                name_local = os.path.basename(name)
                name_remote = os.path.join(folder_save, name)
                print('Trying to save:', name_remote)
                with open(name_local, 'wb') as f:
                    f.write(fl.read())
                gcs.put(name_local, name_remote)
                os.remove(name_local)
                print('Saved:', name_remote)

    @classmethod
    def main(cls, ufs=None):
        if ufs is None:
            ufs = cls.ufs
        for uf in ufs:
            fls = cls.get(uf)
            cls.save(fls)

class Parser:
    pass

class Parse_Domicilios(Parser):
    pass


if __name__ == '__main__':
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--download', action='store_true')
    arguments.add_argument('--ufs', default='')
    parsed = arguments.parse_args()
    
    if parsed.download:
        if parsed.ufs:
            ufs = parsed.ufs.split(',')
        else:
            ufs = None
        FTP_CensoMicrodados.main(ufs=ufs)
