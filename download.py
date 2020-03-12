from contextlib import nullcontext
import io
from pathlib import Path
from functools import partial
import os
from pprint import pprint
import itertools
import sys

import pandas as pd
import requests
from tqdm import tqdm
import dask.dataframe as dd
import bs4


def download_metadata(*, session):
    response = session.get(
        'http://discomap.eea.europa.eu/map/fme/metadata/'
        'PanEuropean_metadata.csv'
    )
    response.raise_for_status()
    meta = pd.read_csv(io.BytesIO(response.content), sep='\t')
    return meta


def download_pollutant_metadata(meta, *, session):
    pollutant_meta = []
    for pollutant_code_url in tqdm(meta['AirPollutantCode'].unique()):
        response = session.get(pollutant_code_url)
        response.raise_for_status()

        def row_filter(tr):
            th = tr.find('th')
            return th.find(string='Notation')

        table_rows = (bs4.BeautifulSoup(response.content)
            .find('div', id='outerframe')
            .find('table')
            .find_all('tr')
        )

        def get_td_text(tr):
            s = ''.join(s.strip() for s in tr.find('td').find_all(text=True))
            return '' if '\n' in s else s   # drop multiline content

        pmeta = {
            tr.find('th').string.strip(): get_td_text(tr)
            for tr in table_rows
        }
        pmeta['AirPollutantCode'] = pollutant_code_url
        pollutant_meta.append(pmeta)

    df = pd.DataFrame(pollutant_meta)
    return df


def download_file_list(pollutant, country_code, *, session):
    url = (
        f'https://fme.discomap.eea.europa.eu/fmedatastreaming/'
        f'AirQualityDownload/AQData_Extract.fmw?CountryCode={country_code}'
        f'&CityName=&Pollutant={pollutant}&Year_from=2013&Year_to=2019&Station='
        f'&Samplingpoint=&Source=E1a&Output=TEXT&UpdateDate=&TimeCoverage=Year'
    )
    response = session.get(url)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content), names=['url'])


def download_data_file(url, *, session):
    response = session.get(url)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content))


def maybe_download(fname, download_func, *, session=None):
    path = Path(fname)
    if not path.exists():
        session_ctx = (
            requests.Session()
            if session is None
            else nullcontext(session)
        )
        with session_ctx as s:
            data = download_func(session=s)
        os.makedirs(path.parent, exist_ok=True)
        data.to_csv(path, index=False)
    else:
        data = pd.read_csv(path)
    return data


def main(argv=sys.argv[1:]):
    data_root = Path(argv[0] if len(argv) >= 1 else 'data')
    with requests.Session() as session:
        print('Metadata...')
        meta = maybe_download(
            data_root / Path('metadata.csv.xz'),
            download_metadata,
            session=session,
        )

        print('Metadata for pollutants...')
        pollutant_meta = maybe_download(
            data_root / Path('metadata_pollutants.csv.xz'),
            partial(download_pollutant_metadata, meta),
            session=session,
        )

        meta = pd.merge(meta, pollutant_meta, on='AirPollutantCode', how='left')

        print('File lists...')
        file_parameters = set(
            (r['Notation'], r['Countrycode'])
            for _, r in meta.iterrows()
        )
        for pollutant, country_code in tqdm(file_parameters):
            try:
                file_list = maybe_download(
                    data_root / Path(
                        f'file_lists/{pollutant}_{country_code}.csv.xz'),
                    partial(download_file_list, pollutant, country_code),
                    session=session,
                )
            except requests.HTTPError as e:
                print(e)

        print('Measurement data...')
        files = dd.read_csv(
            data_root / Path('file_lists/*.csv.xz'),
            include_path_column=True,
            compression='xz',
            blocksize=None,
        ).compute()
        for _, row in tqdm(list(files.iterrows())):
            try:
                url = row['url']
                pollutant, country_code = (
                    Path(row['path']).name.split('.')[0].split('_'))
                path = (
                    data_root / Path(f'raw/{pollutant}/{country_code}')
                    / (Path(url).name + '.xz')
                )
                maybe_download(
                    path,
                    partial(download_data_file, url),
                    session=session,
                )
            except requests.HTTPError as e:
                print(f'error downloading {path} from {url}: {e}')
            except Exception as e:
                print(f'error processing {url}: {e}')

        print('Done.')

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
