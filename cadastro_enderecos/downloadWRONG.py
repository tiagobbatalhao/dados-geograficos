import os
import urllib.request
import zipfile
import io
import pandas
import geopandas
import glob
import argparse

class FTP_CadastroEnderecos:
    """
    Read list of district codes
    """
    url = 'ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Cadastro_Nacional_de_Enderecos_Fins_Estatisticos/'

    layout = [
        ('Código da UF', 1, 2),
        ('Código do município', 3, 5),
        ('Código do distrito', 8, 2),
        ('Código do subdistrito', 10, 2),
        ('Código do setor', 12, 4),
        ('Situação do setor', 16, 1),
        ('Tipo do logradouro', 17, 20),
        ('Título do logradouro', 37, 30),
        ('Nome do logradouro', 67, 60),
        ('Número no logradouro', 127, 8),
        ('Modificador do número', 135, 7),
        ('Elemento 1', 142, 20),
        ('Valor 1', 162, 10),
        ('Elemento 2', 172, 20),
        ('Valor 2', 192, 10),
        ('Elemento 3', 202, 20),
        ('Valor 3', 222, 10),
        ('Elemento 4', 232, 20),
        ('Valor 4', 252, 10),
        ('Elemento 5', 262, 20),
        ('Valor 5', 282, 10),
        ('Elemento 6', 292, 20),
        ('Valor 6', 312, 10),
        ('Latitude', 322, 15),
        ('Longitude', 337, 15),
        ('Localidade', 352, 60),
        ('Nulo', 412, 60),
        ('Espécie de endereço', 472, 2),
        ('Identificação estabelecimento', 474, 40),
        ('Indicador de endereço', 514, 1),
        ('identificação domicílio coletivo', 515, 30),
        ('Número da quadra (*)', 545, 3),
        ('Número da face', 548, 3),
        ('CEP', 551, 8),
    ]
    dicionarios = {
        'Situação do setor': {
            '1': 'urbano',
            '2': 'rural',
        },
        'Espécie de endereço': {
            '01': 'domicílio particular',
            '02': 'domicílio coletivo',
            '03': 'estabeleciemento agropecuário',
            '04': 'estabelecimento de ensino',
            '05': 'estabelecimento de saúde',
            '06': 'estabeleciemento de outras finalidades',
            '07': 'edificação em construção',
        },
        'Indicador de endereço': {
            '1': 'único',
            '2': 'múltiplo',
        },
    }

    states = (
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
        states_dict = {x[0]: x[1] for x in cls.states}
        uf_id = states_dict.get(uf.upper())
        url = os.path.join(cls.url, uf.upper(), str(uf_id)+'.zip')        
        with urllib.request.urlopen(url) as req:
            print(req.url)
            content = req.read()
        try:
            zipped = zipfile.ZipFile(io.BytesIO(content))
            files = {x: zipped.open(x) for x in zipped.namelist()}
            return files
        except (zipfile.BadZipFile, NotImplementedError):
            fname = str(uf_id)+'.zip'
            print('Could not unzip:', fname)
            return {fname: io.BytesIO(content)}

    # @classmethod
    # def parse(cls, filename):
    #     with open(filename, 'r', encoding='latin1') as f:
    #         lines = f.readlines()
    #     df = pandas.DataFrame(lines, columns=['line'])
    #     for layout in cls.layout:
    #         start = layout[1] - 1
    #         stop = start + layout[2]
    #         df[layout[0]] = df['line'].str.slice(start, stop)
    #     df = df.drop('line', axis=1)
    #     for col, d in cls.dicionarios.items():
    #         df[col] = df[col].apply(lambda x: d.get(x, x))
    #     for col in df.columns:
    #         df[col] = df[col].str.strip()
    #     return df

    @classmethod
    def parse(cls, lines):
        df = pandas.DataFrame(lines, columns=['line'])
        for layout in cls.layout:
            start = layout[1] - 1
            stop = start + layout[2]
            df[layout[0]] = df['line'].str.slice(start, stop)
        df = df.drop('line', axis=1)
        for col, d in cls.dicionarios.items():
            df[col] = df[col].apply(lambda x: d.get(x, x))
        for col in df.columns:
            df[col] = df[col].str.strip()
        return df

    @classmethod
    def download_and_parse(cls, uf):
        states_dict = {x[0]: x[1] for x in cls.states}
        uf_id = states_dict.get(uf.upper())
        url = os.path.join(cls.url, uf.upper(), str(uf_id)+'.zip')        
        with urllib.request.urlopen(url) as req:
            print(req.url)
            content = req.read()
        try:
            dataframes = {}
            zipped = zipfile.ZipFile(io.BytesIO(content))
            for name in zipped.namelist():
                flbytes = zipped.open(name, 'r').read()
                with io.BytesIO(flbytes) as f:
                    lines = [x.decode('latin1') for x in f.readlines()]
                dataframes[name] = cls.parse(lines)                
            return True, dataframes
        except (zipfile.BadZipFile, NotImplementedError):
            print('Could not unzip:', uf)
            return False, io.BytesIO(content)





def main_raw():

    folder_save = os.path.join(
        os.path.expanduser('~/localdatalake'),
        'geograficos',
        'cadastro_enderecos_raw',
    )

    ufs = [
        'ba', 'se', 'al', 'pe', 'rn', 'pb', 'ce', 'pi', 'ma',
        'go', 'mt', 'ms', 'df',
        'sp', 'rj', 'es', 'mg',
        'rs', 'sc', 'pr',
        'ap', 'am', 'ac', 'pa', 'ro', 'rr', 'to',
    ]
    for uf in ufs:
        fls = FTP_CadastroEnderecos.get(uf)
        for name, fl in fls.items():
            fname = os.path.join(folder_save, name)
            folder_this = os.path.dirname(fname)
            if not os.path.exists(folder_this):
                os.makedirs(folder_this)
            with open(fname, 'wb') as flsave:
                flsave.write(fl.read())


def main_parse():

    folder_save = os.path.join(
        os.path.expanduser('~/localdatalake'),
        'geograficos',
        'cadastro_enderecos_refined',
    )

    fls = glob.glob(os.path.join(folder_save, '*.txt'))
    for fl in fls:
        print('Reading {}'.format(fl))
        with open(fl, encoding='latin1') as f:
            lines = f.readlines()
        df = FTP_CadastroEnderecos.parse(lines)
        fsave = os.path.join(folder_save, os.path.basename(fl).split('.')[0]+'.csv.gz')
        folder = os.path.dirname(fsave)
        if not os.path.exists(folder):
            os.makedirs(folder)
        try:
            df.to_csv(
                fsave,
                index=False,
                header=True,
                compression='gzip',
            )
            print('Saved to {}'.format(fsave))
        except KeyboardInterrupt:
            if os.path.exists(fsave):
                os.remove(fsave)


def main_download_and_parse():

    folder_save = os.path.join(
        os.path.expanduser('~/localdatalake'),
        'geograficos',
        'cadastro_enderecos_refined',
    )

    ufs = [
        'go', 'mt', 'ms', 'df',
        'rs', 'sc', 'pr',
        'sp', 'rj', 'es', 'mg',
        'ba', 'se', 'al', 'pe', 'rn', 'pb', 'ce', 'pi', 'ma',
        'ap', 'am', 'ac', 'pa', 'ro', 'rr', 'to',
    ]

    for uf in ufs:
        success, dfs = FTP_CadastroEnderecos.download_and_parse(uf)
        if success:
            for key, df in dfs.items():
                fsave = os.path.join(folder_save, key.split('.')[0] + '.csv.gz')
                folder_this = os.path.dirname(fsave)
                if not os.path.exists(folder_this):
                    os.makedirs(folder_this)
                try:
                    df.to_csv(
                        fsave,
                        index=False,
                        header=True,
                        compression='gzip',
                    )
                    print('Saved to {}'.format(fsave))
                except KeyboardInterrupt:
                    if os.path.exists(fsave):
                        os.remove(fsave)
        else:
            fname = os.path.join(folder_save, '{}.zip'.format(uf))
            folder_this = os.path.dirname(fname)
            if not os.path.exists(folder_this):
                os.makedirs(folder_this)
            with open(fname, 'wb') as flsave:
                flsave.write(dfs.read())
            print('Saved {}'.format(fname))

if __name__ == '__main__':
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--download', action='store_true')
    arguments.add_argument('--parse', action='store_true')
    arguments.add_argument('--download_and_parse', action='store_true')
    parsed = arguments.parse_args()
    
    if parsed.download:
        main_raw()
    if parsed.parse:
        main_parse()
    if parsed.download_and_parse:
        main_download_and_parse()
