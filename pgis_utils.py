import datetime
from typing import Callable, Protocol, cast
import folium
import shapely
import geopandas as gpd
from psql_utils.epsql import Engine

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
