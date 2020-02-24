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
    url = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados/'
    folder_save = os.path.join(
        'gs://tb-dados_geograficos',
        'raw',
        'censo_microdados',
    )

    ufs = [
        'sp1', 'sp2_rm', 'rj', 'es', 'mg',
        'rs', 'sc', 'pr',
        'ap', 'am', 'ac', 'pa', 'ro', 'rr', 'to',
        'ba', 'se', 'al', 'pe', 'rn', 'pb', 'ce', 'pi', 'ma',
        'go', 'mt', 'ms', 'df',
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
        folder_save = cls.folder_save
        gcs = connections.get_GCS_client()
        for name, fl in files_dict.items():
            if not name.endswith('/'):
                name_local = os.path.basename(name)
                name_remote = os.path.join(folder_save, name)
                print('Trying to save:', name_remote)
                with open(name_local, 'wb') as f:
                    f.write(fl.read())
                try:
                    gcs.put(name_local, name_remote)
                except:
                    if gcs.exists(name_remote):
                        gcs.delete(name_remote)
                os.remove(name_local)
                print('Saved:', name_remote)

    @classmethod
    def main(cls, ufs=None):
        if ufs is None:
            ufs = cls.ufs
        for uf in ufs:
            fls = cls.get(uf)
            cls.save(fls)

    @classmethod
    def check(cls):
        folder_save = cls.folder_save
        gcs = connections.get_GCS_client()
        files = gcs.glob(os.path.join(folder_save, '*', '*.txt'))
        filetypes = [
            'Pessoas',
            'Domicilios',
            'Emigracao',
            'Mortalidade',
        ]
        correct = []
        ufs = set([x.split('/')[-2] for x in files])
        for uf in sorted(ufs):
            files_this = [x for x in files if x.split('/')[-2]==uf]
            for type_ in filetypes:
                contains = any([type_ in x for x in files_this])
                if not contains:
                    break
            correct.append(uf)
        fix_name = lambda s: s.upper().replace('_', '-')
        remaining = [fix_name(x) for x in cls.ufs if fix_name(x) not in correct]
        return files, correct, remaining

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
            check = FTP_CensoMicrodados.check()
            ufs = check[2]
        FTP_CensoMicrodados.main(ufs=ufs)
