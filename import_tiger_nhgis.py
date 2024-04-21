#%%

import glob, os, psycopg2, re, sys
import geopandas as gpd

from utils import utils
from psql_utils import epsql

#%%
tiger_downloads = 'tiger_downloads'

class GeographyTable:
    year: int
    level_name: str
    download_subdir: bool
    geoid_column_name: str
    download_by_state: bool
    def __init__(self, level_name, year, download_subdir=None, download_by_state=True, geoid_column_name="geoid"):
        self.level_name = level_name
        self.year = year
        if download_subdir == None:
            download_subdir = level_name.upper()
        self.download_subdir = download_subdir
        self.geoid_column_name = geoid_column_name
        self.download_by_state = download_by_state

    def shapefile_path(self, state_fips):
        name = self.geography_name(state_fips)
        dest = f'{tiger_downloads}/{name}'

        if not os.path.exists(dest):
            src = self.src(state_fips)

            utils.download_file(src, f'{dest}.zip')
            utils.unzip_file(f'{dest}.zip')
            os.unlink(f'{dest}.zip')
        
        shapefile_candidates = glob.glob(f'{dest}/*.shp')
        assert len(shapefile_candidates) == 1
        return shapefile_candidates[0]

    def read_shapefile_as_wgs84(self, state_fips: str):
        gdf = gpd.read_file(self.shapefile_path(state_fips))
        epsql.sanitize_column_names(gdf, inplace=True)
        gdf.rename_geometry('geom', inplace=True)
        gdf.to_crs(epsg=4326, inplace = True)
        return gdf

    @staticmethod
    def geography_tables(year):
        # county = TigerLevel('county', download_subdir="COUNTY", download_by_state=False)
        # tract = TigerLevel("tract")
        # blockgroup = TigerLevel("bg")
        # #decade = int((year % 100) / 10) * 10
        # decade = 20
        # block = TigerLevel(f"tabblock{decade}")
        # return [county, tract, blockgroup, block]

        if year == 2000:
            return [
                GeographyTable('tract90', year, type='NHGIS', geoid_column_name="ctidfp00")
            ]
        if year == 2010:
            return [
                GeographyTable('tract00', year, download_subdir="TRACT/2000", geoid_column_name="ctidfp00"),
                GeographyTable('tract10', year, download_subdir="TRACT/2010", geoid_column_name="geoid10"),
                GeographyTable('bg00', year, download_subdir="BG/2000", geoid_column_name="bkgpidfp00"),
                GeographyTable('bg10', year, download_subdir="BG/2010", geoid_column_name="geoid10"),
                GeographyTable('tabblock00', year, download_subdir="TABBLOCK/2000", geoid_column_name="blkidfp00"),
                GeographyTable('tabblock10', year, download_subdir="TABBLOCK/2010", geoid_column_name="geoid10"),
                GeographyTable('county00', year, download_subdir="COUNTY/2000", download_by_state=False, geoid_column_name="cntyidfp00"),
                GeographyTable('county10', year, download_subdir="COUNTY/2010", download_by_state=False, geoid_column_name="geoid10"),
                GeographyTable('state00', year, download_subdir="STATE/2000", download_by_state=False, geoid_column_name="statefp00"),
                GeographyTable('state10', year, download_subdir="STATE/2010", download_by_state=False, geoid_column_name="geoid10")
            ]
        elif year == 2020:
            return [
                GeographyTable('tract', year),
                GeographyTable('bg', year),
                GeographyTable('tabblock10', year, download_subdir="TABBLOCK/2010", geoid_column_name="geoid10"
    ),
                GeographyTable('tabblock20', year, download_subdir="TABBLOCK/2020", geoid_column_name="geoid20"),
                GeographyTable('county', year, download_by_state=False),
                GeographyTable('state', year, download_by_state=False),
            ]
        else:
            return [
                GeographyTable('tract', year), 
                GeographyTable('bg', year), 
                GeographyTable('county', year, download_by_state=False),
                GeographyTable('state', year, download_by_state=False),
            ]
    
    def geography_name(self, state_fips: str):
        return f'tl_{self.year}_{state_fips}_{self.level_name}'
    
    def geography_table_name(self):
        return f'tiger_wgs84.tl_{self.year}_{self.level_name}'

    def src(self, state_fips: str):
        src = (
            f'https://www2.census.gov/geo/tiger/TIGER{self.year}/{self.download_subdir}/'
            f'{self.geography_name(state_fips)}.zip'
        )
        return src

    def index_geography_geometries(self, engine):
        table_name = self.geography_table_name()
        schema = epsql.get_schema(table_name)
        with engine.connect() as con:
            con.execute(f'CREATE INDEX IF NOT EXISTS {epsql.get_table_name(table_name)}_geom_idx ON {table_name} USING GIST (geom)')
            con.execute(f'CREATE INDEX IF NOT EXISTS {epsql.get_table_name(table_name)}_geoid_idx ON {table_name} ({self.geoid_column_name})')

    def load_geography_geometries(self, engine, drop_first=False):
        table_name = self.geography_table_name()
        schema = epsql.get_schema(table_name)
        if schema != 'public':
            engine.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        if drop_first:
            engine.execute(f'DROP TABLE IF EXISTS {table_name}')
        if self.level_name.endswith('10') or self.level_name.endswith('20'):
            suffix=self.level_name[-2:]
        else:
            suffix=''
        with engine.connect() as con:
            already_loaded = []
            if self.download_by_state:
                fips_list = all_state_fips
            else:
                fips_list = ['us']
            for fips in fips_list:
                if self.download_by_state:
                    state_filter = f"WHERE {self.geoid_column_name} >= '{fips}' and {self.geoid_column_name} < '{fips}z'"
                else:
                    state_filter = ''
                if con.table_exists(table_name) and con.execute_exists(f"SELECT EXISTS (SELECT {self.geoid_column_name} FROM {table_name} {state_filter} LIMIT 1)"):
                    already_loaded.append(fips)
                else:
                    gdf = self.read_shapefile_as_wgs84(fips)
                    gdf.to_postgis(
                        epsql.get_table_name(table_name),
                        con, 
                        schema = schema,
                        if_exists='append')
                    print(f'{table_name}: Added {len(gdf)} records from FIPS {fips}')
                    self.index_geography_geometries(engine)
            if already_loaded:
                print(f'{table_name}: Already loaded FIPS {", ".join(already_loaded)}')
            self.index_geography_geometries(engine)

all_state_fips = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "60",
    "66",
    "69",
    "72",
    "78"
]



#%%

def add_census_geoids(engine, dest_table, dest_geom_column, year, verbose=False):
    print(f'Adding census geoids to {dest_table}.{dest_geom_column} from TIGER year {year}')
    for geography in GeographyTable.geography_tables(year):
        census_table = geography.table_name()
        census_geoid_column = geography.geoid_column_name
        dest_geoid_column = f'{dest_geom_column}_{geography}_{census_geoid_column}'
        engine.execute(f'ALTER TABLE {dest_table} DROP COLUMN IF EXISTS {dest_geoid_column}')
        engine.execute(f'ALTER TABLE {dest_table} ADD COLUMN {dest_geoid_column} TEXT')
        cmd = f"""
            UPDATE {dest_table} AS dest
            SET {dest_geoid_column} = tiger.{census_geoid_column}
            FROM {census_table} AS tiger
            WHERE ST_Contains(tiger.geom, dest.{dest_geom_column})"""
        engine.execute(cmd, verbose=verbose)
        geoid_count = engine.execute_count(f'SELECT COUNT({dest_geoid_column}) FROM {dest_table}')
        all_count = engine.execute_count(f'SELECT COUNT(*) FROM {dest_table}')
        print(f'  Created {dest_table}.{dest_geoid_column}, finding {geoid_count} of {all_count} records')
# %%
