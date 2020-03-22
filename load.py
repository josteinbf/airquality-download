from typing import Union, Iterable
from pathlib import Path
from glob import glob
import logging
import argparse
import os
import itertools
import sys

import pandas as pd
import sqlalchemy
from tqdm import tqdm
from stringcase import snakecase
from d6tstack.utils import pd_to_psql
import psycopg2.errors


log = logging.getLogger(__name__)


def normalize_name(n):
    return snakecase(n.replace(' ', '_'))


def normalize_column_names(d):
    return d.rename(columns={n: normalize_name(n) for n in d.columns})


def load_metadata(fname: Union[Path, str], db_connection: str):
    db = sqlalchemy.create_engine(db_connection)
    d = pd.read_csv(fname, na_values=['', -999])
    d = normalize_column_names(d)
    log.debug(d.columns)
    d.to_sql(
        'station', db, if_exists='replace', index_label='station_id',)
    with db.connect() as con:
        con.execute('ALTER TABLE station ADD PRIMARY KEY (station_id);')
    log.info(f'Wrote stations from file {fname} into table `station\'.')


def load_quantities(fname: Union[Path, str], db_connection: str):
    # Concept URI,Preferred label,Definition,Notation,Status,Status Modified,
    # Accepted Date,Not Accepted Date,Alternative label,
    # Scope note,Recommended unit,Measurement equipment,Has protection target,
    # AirPollutantCode,Has related match
    db = sqlalchemy.create_engine(db_connection)
    d = pd.read_csv(fname)
    d = normalize_column_names(d)
    log.debug(d.columns)
    d.to_sql('quantity', db, if_exists='replace', index_label='quantity_id')
    with db.connect() as con:
        con.execute('ALTER TABLE quantity ADD PRIMARY KEY (quantity_id);')
    log.info(f'Wrote pollutants from file {fname} into table `quantity\'.')


def load_observations(
        fnames: Iterable[Union[Path, str]], db_connection: str,
        *, progress: bool = True):
    # Countrycode,Namespace,AirQualityNetwork,AirQualityStation,
    # AirQualityStationEoICode,SamplingPoint,SamplingProcess,Sample,
    # AirPollutant,AirPollutantCode,AveragingTime,Concentration,
    # UnitOfMeasurement,DatetimeBegin,DatetimeEnd,Validity,Verification
    db = sqlalchemy.create_engine(db_connection)

    station = pd.read_sql_query(
        'select station_id, air_quality_station from station;',
        db,
    )
    quantity = pd.read_sql_query(
        '''
        select quantity_id, air_pollutant_code, recommended_unit
        from quantity;
        ''',
        db,
    )
    log.debug(quantity.head())

    write_cols = [
        'Concentration',
        'DatetimeBegin',
        'DatetimeEnd',
        'Validity',
        'Verification',
    ]
    extra_cols = [
        'AirQualityStation',
        'AirPollutantCode',
        'UnitOfMeasurement',
    ]

    # Get the list of columns from the DB; we need to have our columns in the
    # correct order for the pd_to_psql function to work correctly
    db_meta = sqlalchemy.MetaData(db)
    db_meta.reflect()
    observation_columns = list(db_meta.tables['observation'].columns.keys())

    fnames = sorted(fnames)
    progress_bar = tqdm if progress else lambda x: x

    for _, fname in progress_bar(list(zip(range(5), fnames))):
        try:
            exc = None
            for encoding in ('utf-8', 'utf-16'):
                try:
                    d = pd.read_csv(
                        fname,
                        usecols=write_cols + extra_cols,
                        parse_dates=['DatetimeBegin', 'DatetimeEnd'],
                        infer_datetime_format=True,
                        encoding=encoding,
                    )
                except UnicodeDecodeError as e:
                    exc = e
                    log.exception(f'error reading {fname}: ')
                else:
                    exc = None
                    break
            else:
                raise Exception(f'Error reading {fname}: {exc}')

            d = normalize_column_names(d)
            d = (
                d
                .merge(station, on='air_quality_station')
                .merge(quantity, on='air_pollutant_code')
                # .query('unit_of_measurement == recommended_unit')
                [observation_columns]
            )
            log.debug(f'NULLs in station_id? {d["station_id"].isna().any()}')
            log.debug(f'NULLs in quantity_id? {d["quantity_id"].isna().any()}')
            log.debug(f'dataframe length: {len(d)}')

            # Insert into DB: the pd_to_psql method is considerably faster.
            # d.to_sql(
            #     'observation', db, if_exists='append', index=False,
            #     method='multi',
            # )
            # NOTE: this function requires the columns in the dataframe to be
            # in the same order as in the DB schema! We take care of this by
            # reading the DB schema before entering the for loop
            pd_to_psql(d, db_connection, 'observation', if_exists='append')
        except psycopg2.errors.UniqueViolation:
            log.warning(
                f'one or more rows in file {fname} are already in the '
                f'database; skipping the entire file.'
            )
        except Exception:
            log.exception(f'error occured for file {fname}: ')
            raise


def load_meta_cmd(args: argparse.Namespace):
    load_metadata(args.stations, args.db_connection)
    load_quantities(args.pollutants, args.db_connection)

    create_table_sql = Path('./create_table_observation.sql').read_text()
    db = sqlalchemy.create_engine(args.db_connection)
    with db.connect() as conn:
        _ = conn.execute(create_table_sql)
    log.info('Created empty table `observation\'.')

    return 0


def load_observations_cmd(args: argparse.Namespace):
    load_observations(
        itertools.chain.from_iterable(glob(f) for f in args.files),
        args.db_connection,
        progress=True,
    )
    return 0


def main(argv=sys.argv):
    parser = argparse.ArgumentParser(
        Path(__file__).name,
        description='Load European air quality data into TimescaleDB',
    )
    db_connection = os.getenv('TIMESCALEDB_CONNECTION')
    if db_connection is None:
        print('error: Environment variable TIMESCALEDB_CONNECTION not set.')
        return 2
    parser.set_defaults(db_connection=db_connection)
    parser.add_argument(
        '--no-progress', dest='progress', action='store_false',
        default=True, help='do not show progress bars',
    )
    parser.add_argument(
        '--verbose', '-v', action='store_const', dest='log_level',
        const=logging.DEBUG, default=logging.INFO,
    )
    subparsers = parser.add_subparsers(help='command')

    meta_parser = subparsers.add_parser('meta', help='load metadata')
    meta_parser.set_defaults(func=load_meta_cmd)
    meta_parser.add_argument(
        'stations', type=Path, help='csv file listing stations')
    meta_parser.add_argument(
        'pollutants', type=Path, help='csv file listing pollutants')

    observation_parser = subparsers.add_parser(
        'observations', help='load observations')
    observation_parser.set_defaults(func=load_observations_cmd)
    observation_parser.add_argument(
        'files', nargs=argparse.REMAINDER,
        help=(
            'csv files (can be compressed; globs allowed) containing '
            'observations'
        ),
    )

    args = parser.parse_args(argv[1:])
    logging.basicConfig(level=args.log_level)
    if 'func' not in args:
        parser.print_usage()
        return 1
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
