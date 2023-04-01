import argparse, os, re, shapely
from typing import cast
import datetime
import geopandas as gpd

from psql_utils.pgis_utils import intersect_all
from psql_utils.epsql import Engine
from utils.read_kml import read_kml_using_mapbox
engine = Engine()

parser = argparse.ArgumentParser()

parser.add_argument('--levels', type=str, default='tract,blockgroup,block', help='Comma-separated list of levels to intersect with (e.g. county,tract,blockgroup,block)')
parser.add_argument('--geom-year', type=int, required=True, help='Year of the geometries to intersect with (e.g. 1990,2000,2010,2015,2020...)')
parser.add_argument('--basis-year', type=int, required=False, help='TIGER basis year of the geometries to intersect with (default: most recent year available)')
parser.add_argument('--filter-name', type=str, required=False, help='Only include layers whose name includes regex')
parser.add_argument('--output-directory', type=str, default='.', help='Output directory')
parser.add_argument('--override-geom-file-srid', type=int, required=False)
# Make argument to capture all remaining arguments
parser.add_argument('files', nargs='+', help='Geometry files (e.g. geojson/shapefile/kml) to intersect with')

args = parser.parse_args()
if args.basis_year:
    basis_years = [cast(int, args.basis_year)]
else:
    basis_years = list(reversed(range(args.geom_year, datetime.datetime.now().year+1)))

for file in args.files:
    print("Processing", file)

    for level in args.levels.split(','):
        if level not in ('county','tract','blockgroup','block'):
            raise Exception(f"Invalid level {level}")

        for basis_year in basis_years:
            table_name = f'{level}_{args.geom_year}_tl{basis_year}'
            geo_table = f'nhgis_geo_wgs84.{table_name}'
            if engine.table_exists(geo_table):
                break
        else:
            raise Exception(f"Could not find any geometry table matching {level}_{args.geom_year}_tl{basis_years}")

        msg = f"Intersecting with geo table {geo_table}"
        if args.basis_year:
            msg += f" (using specified basis year {basis_year})"
        else:
            msg += f" (using most recent basis year found {basis_year})"

        print(msg)

        if file.endswith('.kml'):
            gdf = read_kml_using_mapbox(file)
        else:
            gdf = gpd.read_file(file)
        print("Read", len(gdf), "layers (rows) from", file)
        if args.filter_name:
            gdf = gdf[gdf['name'].str.contains(args.filter_name)]
            print(f"Filtered to {len(gdf)} layers (rows) using filter-name '{args.filter_name}'")
        print(gdf)


        def sanitize_filename(filename: str):
            filename = re.sub(r'^\W+', '', filename) # Remove leading non-word-chars
            filename = re.sub(r'\W+$', '', filename) # Remove trailing non-word-chars
            filename = re.sub(r'\W+', '_', filename) # Replace contiguous sets of non-word-chars with underscore
            return filename.lower()

        #intersect_all(engine, geom, geo_table)

        gdf_srid = gdf.crs and gdf.crs.to_epsg()
        if args.override_geom_file_srid:
            print(f"Overriding source geometry SRID from {gdf_srid} to {args.override_geom_file_srid}")
            gdf_srid = args.override_geom_file_srid
        else:
            print(f"Source geometry SRID is {gdf_srid}")
        src_name = os.path.splitext(os.path.basename(file))[0]

        for name in dict.fromkeys(gdf['name']).keys():
            geoms = gdf[gdf['name'] == name].geometry
            geoms = [geom.buffer(0) for geom in geoms]
            if len(geoms) == 1:
                geom = geoms.iloc[0]
            else:
                print("Unioning", len(geoms), "geometries for", name)
                geom = shapely.ops.unary_union(geoms)

            if geom == None:
                print(f"Skipping {name} because it has no geometry")
                continue
            if shapely.get_srid(geom) == 0:
                geom = shapely.set_srid(geom, gdf_srid)
            int_gdf = intersect_all(engine, geom, geo_table)

            filename = f"{args.output_directory}/{table_name}_{src_name}_{sanitize_filename(name)}.geojson"
            int_gdf.to_file(filename, driver='GeoJSON')
            print(f"{filename}: Wrote {len(int_gdf)} intersections")



