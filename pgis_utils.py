import datetime, os
from typing import Callable, Protocol, cast
import folium
import shapely
import geopandas as gpd
from psql_utils import epsql
from psql_utils.epsql import Engine
from utils import utils

from shapely import Point
from shapely.geometry import Polygon, MultiPolygon, LineString, MultiLineString, GeometryCollection
from shapely.geometry.base import BaseGeometry
from shapely import wkb

# Get rid of this once shapely has real typing
get_srid = cast(Callable[[BaseGeometry], int], shapely.get_srid)

# Get rid of this once geopandas has real typing
class GeoDataFrameProtocol(Protocol):
    def __call__(self, geometry: list[BaseGeometry], crs: int) -> gpd.GeoDataFrame: ...
GeoDataFrameConstructor = cast(GeoDataFrameProtocol, gpd.GeoDataFrame)

def explore_geoms(geoms: list[BaseGeometry]) -> folium.Map:
    for geom in geoms:
        assert shapely.get_srid(geom) == 4326
    get_srid(geoms[0])

    gdf = GeoDataFrameConstructor(
        geometry=geoms,
        crs=get_srid(geoms[0])
        )
    return gdf.explore()

def explore_geom(geom: BaseGeometry):
    return explore_geoms([geom])

def write_geom(geom: BaseGeometry, filename: str):
    assert shapely.get_srid(geom) == 4326
    gdf = GeoDataFrameConstructor(
        geometry=[geom],
        crs=get_srid(geom)
        )
    gdf.to_file(filename, driver='GeoJSON')
    assert(gpd.read_file(filename).crs.to_epsg() == shapely.get_srid(geom))

def pgval(x: BaseGeometry|str):
    if isinstance(x, BaseGeometry):
        return pggeom(x)
    else:
        assert(isinstance(x, str))
        # TODO: properly quote this string
        return f"'{x}'"

def pggeog(x: BaseGeometry):
    assert(shapely.get_srid(x) == 4326)
    return f"'{wkb.dumps(x, hex=True, include_srid=True)}'::geography"

def pggeom(x: BaseGeometry):
    assert(shapely.get_srid(x) == 4326)
    return f"'{wkb.dumps(x, hex=True, include_srid=True)}'::geometry"

def pgarray(elts):
    return f"ARRAY[{','.join([pgval(elt) for elt in elts])}]"

def st_collect(engine: Engine, geoms):
    return engine.execute_returning_geom(f"select st_collect({pgarray(geoms)}) as geom")

def st_linemerge(engine: Engine, geom):
    return engine.execute_returning_geom(f"select st_linemerge({pgval(geom)}) as geom")

def st_union(engine: Engine, geoms):
    return engine.execute_returning_geom(f"select st_union({pgarray(geoms)}) as geom")

def st_intersection(engine: Engine, geom1, geom2):
    return engine.execute_returning_geom(f"select st_intersection({pgval(geom1)}, {pgval(geom2)}) as geom")

def st_buffer(engine: Engine, geom, radius_in_meters):
    return engine.execute_returning_geom(f"select st_buffer({pggeog(geom)}, {radius_in_meters}) as geom")

def intersect_all(engine: Engine, geom, geo_table):
    return engine.execute_returning_gdf(f"select *, st_area(st_intersection(geom, {pggeom(geom)})::geography) / st_area(geom::geography) as weight from {geo_table} where st_intersects({pggeom(geom)}, geom)")

def find_nhgis_census_table(engine: Engine, level: str, geom_year: int, basis_year: int|None = None):
    if level not in ('county','tract','blockgroup','block'):
        raise Exception(f"Invalid level {level}")
    
    if basis_year:
        basis_years = [cast(int, basis_year)]
    else:
        basis_years = list(reversed(range(geom_year, datetime.datetime.now().year+1)))

    for by in basis_years:
        geo_table = f'nhgis_geo_wgs84.{level}_{geom_year}_tl{by}'
        if engine.table_exists(geo_table):
            return geo_table
    raise Exception(f"Could not find any geometry table matching {level}_{geom_year}_tl{basis_years}")

def get_geom_at_iloc(gdf, iloc):
    geom = gdf.geometry.iloc[iloc]
    if shapely.get_srid(geom) == 0:
        geom = shapely.set_srid(geom, gdf.crs.to_epsg())
    return geom

class GeographySource:
    def __init__(self, table_name_with_schema, id_column_name, url:str|None=None, gdf:gpd.GeoDataFrame|None=None):
        self.table_name_with_schema = table_name_with_schema
        self.table_name = epsql.get_table_name(table_name_with_schema)
        self.schema_name = epsql.get_schema(table_name_with_schema)
        self.id_column_name = id_column_name
        assert self.schema_name
        assert url or gdf is not None
        self.url = url
        self.gdf = gdf

    def local_path(self):
        assert(self.url)
        return os.path.join(self.schema_name, self.table_name + os.path.splitext(self.url)[1])

    def download(self):
        utils.download_file(self.url, self.local_path())

    def to_postgis(self, engine: Engine):
        if self.gdf is not None:
            gdf = self.gdf
            print(f"{self.table_name_with_schema}: gdf has {len(gdf)} rows")
        else:
            self.download()
            gdf = gpd.read_file(self.local_path())
            gdf.to_crs(epsg=4326, inplace=True) # Reproject to WGS84, if not already
            print(f"{self.table_name_with_schema}: Read {len(gdf)} rows from {self.local_path()}")
        epsql.sanitize_column_names(gdf, inplace=True)
        gdf.rename_geometry('geom', inplace=True)
        print(gdf.columns)
        engine.execute(f"create schema if not exists {self.schema_name}")
        gdf.to_postgis(self.table_name, engine.engine, schema=self.schema_name, if_exists='replace', index=False)#, dtype={'geom': 'Geometry'})
        # Make sure geometries are valid
        engine.execute(f"""
            UPDATE {self.table_name_with_schema}
            SET geom=ST_MakeValid(geom)
            WHERE NOT ST_IsValid(geom);""")
        # Create ID index
        if self.id_column_name:
            engine.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS {self.table_name}_id_idx ON {self.table_name_with_schema} ({self.id_column_name});')
        # Create spatial index for both geom and geog
        engine.execute(f'CREATE INDEX IF NOT EXISTS {self.table_name}_geom_idx ON {self.table_name_with_schema} USING GIST (geom);')
        engine.execute(f'CREATE INDEX IF NOT EXISTS {self.table_name}_geog_idx ON {self.table_name_with_schema} USING GIST (geography(geom));')
        print(f"{self.table_name_with_schema}: Created and indexed")

    def create_crosswalk(self, engine: Engine, dest_table_name: str, dest_id_column_name: str):
        if not self.id_column_name:
            print(f"{self.table_name_with_schema}: No ID column, skipping crosswalk")
        else:
            engine.create_highest_overlap_crosswalk(
                dest_table_name, dest_id_column_name,
                self.table_name_with_schema, self.id_column_name)

