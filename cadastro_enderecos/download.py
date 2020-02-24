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

class FTP_CadastroEnderecos:
    url = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Cadastro_Nacional_de_Enderecos_Fins_Estatisticos/'

    folder_save = os.path.join(
        'gs://tb-dados_geograficos',
        'raw',
        'cadastro_enderecos',
    )

    ufs = [
        'go', 'mt', 'ms', 'df',
        'ap', 'am', 'ac', 'pa', 'ro', 'rr', 'to',
        'se', 'al', 'pe', 'rn', 'pb', 'ce', 'pi', 'ma', 'ba',
        'rs', 'sc', 'pr',
        'sp', 'rj', 'es', 'mg',
    ]

    ufids = (
        ('RO', 11),
        ('AC', 12),
        ('AM', 13),
        ('RR', 14),
        ('PA', 15),
        ('AP', 16),
        ('TO', 17),
        ('MA', 21),
        ('PI', 22),
        ('CE', 23),
        ('RN', 24),
        ('PB', 25),
        ('PE', 26),
        ('AL', 27),
        ('SE', 28),
        ('BA', 29),
        ('MG', 31),
        ('ES', 32),
        ('RJ', 33),
        ('SP', 35),
        ('PR', 41),
        ('SC', 42),
        ('RS', 43),
        ('MS', 50),
        ('MT', 51),
        ('GO', 52),
        ('DF', 53),
    )


    @classmethod
    def get(cls, uf):
        states_dict = {x[0]: x[1] for x in cls.ufids}
        uf_id = states_dict.get(uf.upper())
        url = os.path.join(cls.url, uf.upper(), str(uf_id)+'.zip')        
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
        states_dict = {x[1]: x[0] for x in cls.ufids}
        files = gcs.glob(os.path.join(folder_save, '*.txt'))
        correct = [
            states_dict.get(int(os.path.basename(x).split('.')[0]))
            for x in files
        ]
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
    arguments.add_argument('--all', default='')
    parsed = arguments.parse_args()
    
    if parsed.download:
        if parsed.ufs:
            ufs = parsed.ufs.split(',')
        else:
            if parsed.all:
                ufs = None
            else:
                check = FTP_CadastroEnderecos.check()
                ufs = check[2]
        FTP_CadastroEnderecos.main(ufs=ufs)
