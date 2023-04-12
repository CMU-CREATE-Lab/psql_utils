
#%%
import binascii
import random, shapely
import string
import sys
from typing import TYPE_CHECKING, Any, cast
from utils import utils
if TYPE_CHECKING:
    import geopandas as gpd
    import pandas as pd

"""epsql:  Extensions to SQLAlchemy engine and connection

Includes TIGER geocoding, simplified UNIX socket connection, convenience wrappers for execute"""

import functools, os, re, sqlalchemy, threading, time, types

def sanitize_table_name(name: str):
    name = re.sub(r'^[^\w\.]+', '', name) # Remove leading non-{word,"."}-chars
    name = re.sub(r'[^\w\.]+$', '', name) # Remove trailing non-{word,"."}-chars
    name = re.sub(r'[^\w\.]+', '_', name) # Replace contiguous sets of non-{word,"."}-chars with underscore
    return name.lower()

def sanitize_column_name(colname: str):
    colname = re.sub(r'^\W+', '', colname) # Remove leading non-word-chars
    colname = re.sub(r'\W+$', '', colname) # Remove trailing non-word-chars
    colname = re.sub(r'\W+', '_', colname) # Replace contiguous sets of non-word-chars with underscore
    return colname.lower()

def sanitize_column_names(df: "pd.DataFrame", inplace: bool = False):
    return df.rename(columns={c:sanitize_column_name(c) for c in cast(list[str], df.columns)}, inplace=inplace)

# Returns table name portion of table_name_with_optional_schema
def get_table_name(table_name_with_optional_schema: str):
    if '.' in table_name_with_optional_schema:
        return table_name_with_optional_schema.split('.')[-1]
    else:
        return table_name_with_optional_schema

# Returns schema name portion of table_name_with_optional_schema;  'public' if no schema specified
def get_schema(table_name_with_optional_schema: str):
    if '.' in table_name_with_optional_schema:
        return table_name_with_optional_schema.split('.')[0]
    else:
        return 'public'

class ConnectionExtensions(sqlalchemy.engine.base.Connection):
    """Extensions for Connection and Engine

    This class is never instantiated directly, but rather patched in upon construction
    of Engine and Connection objects"""

    def __init__(self):
        raise Exception(
            "Don't instantiate this class directly;  using epsql.Engine will patch these methods "
            "into Connection and Engine instances.")

    def execute(self, *args: Any, verbose: bool = False, **kwargs: Any):
        if verbose:
            print(f'{args[0]}')
        before = time.time()
        # ignore vscode's warning about the return type of execute

        ret=sqlalchemy.engine.base.Connection.execute(self, *args, **kwargs) # type: ignore
        if verbose:
            print(f'Completed in {time.time()-before:.1f} seconds')
        return ret

    def execute_returning_value(self, *args: Any, **kwargs: Any):
        dicts = self.execute_returning_dicts(*args, **kwargs)
        assert(len(dicts) == 1)
        values = list(dicts[0].values())
        assert(len(values) == 1)
        return values[0]
    
    def execute_returning_geom(self, *args: Any, **kwargs: Any) -> shapely.geometry.base.BaseGeometry: # type: ignore
        gdf = self.execute_returning_gdf(*args, **kwargs) # type: ignore
        assert(len(gdf) == 1) # type: ignore
        return gdf.geometry[0] # type: ignore

    def execute_returning_dicts(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        results = self.execute(*args, **kwargs)
        return [dict(rec) for rec in results] # type: ignore

    # For kwargs, see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html
    def execute_returning_df(self, sql: str, **kwargs: Any) -> "pd.DataFrame":
        import pandas as pd # type: ignore
        return pd.read_sql_query(sql, self, **kwargs)

    # For kwargs, see https://geopandas.org/reference/geopandas.read_postgis.html#geopandas.read_postgis
    def execute_returning_gdf(self, sql: str, **kwargs: Any) -> "gpd.GeoDataFrame":
        import geopandas as gpd
        ret = gpd.read_postgis(sql, self, **kwargs)
        assert(isinstance(ret, gpd.GeoDataFrame))
        return ret

    def execute_update(self, *args: Any, **kwargs: Any) -> int:
        return self.execute(*args, **kwargs).rowcount # type: ignore

    def execute_delete(self, *args: Any, **kwargs: Any) -> int:
        return self.execute(*args, **kwargs).rowcount # type: ignore

    #def df_to_table(self, df, table_name, **kwargs):
    #    with Stopwatch(f'Adding {len(df)} records to {table_name}'):
    #        sanitize_column_names(df).to_sql(table_name, self, **kwargs)

    def table_exists(self, table_name: str) -> bool:
        return self.execute_exists(f"""SELECT EXISTS (
            SELECT FROM pg_tables WHERE schemaname='{get_schema(table_name)}' AND tablename='{get_table_name(table_name)}')""")
    
    def list_tables(self, schema: str = 'public') -> list[str]:
        return [r['tablename'] for r in self.execute_returning_dicts(f"SELECT tablename FROM pg_tables WHERE schemaname='{schema}'")]
    
    def list_schema_sizes(self) -> "pd.DataFrame":
        return self.execute_returning_df("""
            SELECT schema_name, 
                pg_size_pretty(sum(table_size)::bigint) as size,
                (sum(table_size) / pg_database_size(current_database())) * 100 as percent,
                sum(table_size)/1e6 as size_mb
            FROM (
            SELECT pg_catalog.pg_namespace.nspname as schema_name,
                    pg_relation_size(pg_catalog.pg_class.oid) as table_size
            FROM   pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
            ) t
            GROUP BY schema_name
            ORDER BY percent DESC""")
    
    def table_column_exists(self, table_name: str, column_name: str) -> bool:
        return column_name in self.table_columns(table_name)

    def table_columns(self, table_name: str) -> list[str]:
        return [c['column_name'] for c in self.execute_returning_dicts(f"""SELECT column_name FROM information_schema.columns WHERE table_name='{get_table_name(table_name)}'""")]
        return self.execute_returning_value(f"SELECT json_object_keys(to_json(json_populate_record(NULL::{table_name}, '{{}}'::JSON)))")

    def table_has_primary_key(self, table_name: str) -> bool:
        #return self.execute_count(f"""SELECT COUNT(*) FROM pg_constraint WHERE conrelid = '{table_name}'::regclass AND contype = 'p'""") > 0
        return self.execute_count(f"""
            SELECT count(*) from information_schema.table_constraints 
                where table_name = '{get_table_name(table_name)}' 
                    and table_schema = '{get_schema(table_name)}'
                    and constraint_type = 'PRIMARY KEY'""") > 0

    def execute_exists(self, sql: str, **kwargs: Any) -> bool:
        return self.execute_returning_dicts(sql, **kwargs)[0]['exists']

    def execute_count(self, sql: str, **kwargs: Any):
        return self.execute_returning_dicts(sql, **kwargs)[0]['count']

    def insert(self, table_name: str, record_dict: dict[str, Any]) -> dict[str, Any]:
        keys = ','.join(record_dict.keys())
        values = ','.join(['%s'] * len(record_dict))
        cmd = f"INSERT INTO {table_name} ({keys}) VALUES ({values}) RETURNING *"
        return self.execute_returning_dicts(cmd, tuple(record_dict.values()))[0]

    # On conflict, do nothing
    def insert_unless_conflict(self, table_name: str, record_dict: dict[str, Any]):
        keys = ','.join(record_dict.keys())
        values = ','.join(['%s'] * len(record_dict))
        cmd = f"INSERT INTO {table_name} ({keys}) VALUES ({values}) ON CONFLICT DO NOTHING"
        return self.execute(cmd, tuple(record_dict.values()))

    # This performs insert, unless there's a conflict on the values contained in index_fields, in which case it will
    # update the conflicting record with values from record_dict
    # Use a unique or primary index on your table to trigger the update instead of insert
    # This uses ON CONFLICT from Postgres
    def upsert(self, table_name: str, index_fields: list[str], record_dict: dict[str, Any]):
        keys = ','.join(record_dict.keys())
        values = ','.join(['%s'] * len(record_dict))
        index_fields_str = ','.join(index_fields)
        cmd = f"""
            INSERT INTO {table_name} ({keys}) VALUES ({values})
            ON CONFLICT ({index_fields_str}) DO UPDATE SET ({keys}) = ({values});"""
        print("in upsert", cmd)
        return self.execute(cmd, tuple(list(record_dict.values()) + list(record_dict.values())))


    def geocode(self, address: str, max_results: int = 1, latlon_only: bool = False):
        if latlon_only:
            sel = """
            SELECT rating, geomout, to_jsonb((addy)) as addy
            FROM geocode(pagc_normalize_address(%(address)s), %(max_results)s) As g;"""
        else:
            sel = """
                SELECT rating, 
                       geomout, 
                       to_jsonb((addy)) as addy,
                       to_jsonb(tabblock.*) - 'the_geom' as block,
                       to_jsonb(bg.*) - 'the_geom' as blockgroup,
                       to_jsonb(tract.*) - 'the_geom' as tract
                FROM geocode(pagc_normalize_address(%(address)s), %(max_results)s) As g
                LEFT JOIN tabblock ON ST_Contains(tabblock.the_geom, geomout)
                LEFT JOIN bg ON ST_Contains(bg.the_geom, geomout)
                LEFT JOIN tract ON ST_Contains(tract.the_geom, geomout)
                """
        return self.execute_returning_dicts(sel, address=address, max_results=max_results)

    def repair_geometries_if_needed(self, table_name: str, geom_column: str = 'geom'):
        print(f'Checking {table_name} for invalid geometries...')
        count = self.execute_count(f'select count(*) from {table_name} where not st_isvalid({geom_column})')
        if count:
            print(f'Repairing {count} geometries in {table_name}')
            cmd = f'update {table_name} set {geom_column} = st_makevalid({geom_column}) where not st_isvalid({geom_column})'
            self.execute(cmd)
        else:
            print('No invalid geometries')

    def add_highest_overlap_crosswalk(
            self, dest_table_name: str, dest_row_id: str, dest_new_col: str, src_table_name: str, src_col: str,
            dest_row_id_min: str|None = None, dest_row_id_max: str|None = None):
        """Create a geographic crosswalk mapping each destination record to a single source record.
        If multiple source records overlap a destination record, the one with the largest area overlap is chosen.
        A source record may be recorded as a match to any number of destination records, including 0.
        Typically the geographic entities in the destination table are smaller than those of the source table.

        Parameters:
        dest_table_name:  Name of destination table, including schema if any.
        dest_row_id:      A unique, indexed row for destination table usable for join.  Typically geoid for census.
        dest_new_col:     Name of column to be created and filled with ID of source record with highest overlap.
        src_table_name:   Name of source table, including schema if any.
        src_col:          Name of unique ID for source record, to be filled into dest_new_col for matching dest record.
                            Typically geoid for census.
        dest_row_id_min:  (optional)  Minimum dest_row_id to update (for updating subset of table)
        dest_row_id_max:  (optional)  Maximum dest_row_id to update (for updating subset of table)

        Performance considerations:
        Both source and destination tables should have spatially indexed geometries.
        This function is fairly performant, able to crosswalk ~11M blocks to ~2K tracts in around two hours on 2010-era server.
        """

        # Assumes dest_new_col should be text
        print(f'Adding {dest_table_name}.{dest_new_col} as crosswalk to {src_table_name}.{src_col} selecting highest overlap...')
        self.execute(f'alter table {dest_table_name} add if not exists {dest_new_col} text;')
        tmp_table_name = f"tmp_crosswalk_{random.getrandbits(64):016x}"

        where_clauses: list[str] = []
        if dest_row_id_min:
            where_clauses.append(f"dest.{dest_row_id} >= '{dest_row_id_min}'")
        if dest_row_id_max:
            where_clauses.append(f"dest.{dest_row_id} <= '{dest_row_id_max}'")
        where_cond = " and ".join(where_clauses)

        cmd = f"""
            create temp table {tmp_table_name} on commit drop as
                select distinct on (dest_id) dest.{dest_row_id} as dest_id, src.{src_col} as src_id
                    from {src_table_name} as src
                    join {dest_table_name} as dest
                    on st_intersects(src.geom, dest.geom) and not st_touches(src.geom, dest.geom)
                    {f'where {where_cond}' if where_cond else ''}
                    order by dest_id, st_area(st_intersection(src.geom, dest.geom)) desc;
            create index {tmp_table_name}_idx on {tmp_table_name} (dest_id);
            update {dest_table_name} as dest
                set {dest_new_col} = tmp.src_id
            from {tmp_table_name} as tmp
            where dest.{dest_row_id} = tmp.dest_id
                {f'and {where_cond}' if where_cond else ''};
        """
        nrows = self.execute_update(cmd, verbose=True)
        print(f'Created {nrows} crosswalk entries')

def _with_connect(engine, member_name, *args, **kwargs): # type: ignore
    with engine.connect() as con: # type: ignore
        return getattr(con, member_name)(*args, **kwargs) # type: ignore

def _find_pghost():
    candidates = [
        "/host-postgresql", # talk to host's postgresql from docker (cocalc)
        "/var/run/postgresql"  # Ubuntu default location
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    raise Exception(f'Attempting to find unix socket for postgresql, but cannot find any of {candidates}')

class Engine(ConnectionExtensions):
    def __init__(self, engine: Any = None, db_name: str = "earthtime", verbose: bool = True):
        if not engine:

            # cocalc sets PGUSER to something unhelpful
            if 'PGUSER' in os.environ:
                del os.environ['PGUSER']

            # TODO: This link shows how to encode a unix socket path into a progresql:/// url, and would
            # probably let us simplify this code somewhat
            # https://stackoverflow.com/questions/27037990/connecting-to-postgres-via-database-url-and-unix-socket-in-rails

            # OLD: There's no syntax available for embedding a socket directory path in the postgresql:/// url,
            # so we need to set instead using environment variable
            os.environ["PGHOST"] = _find_pghost()

            if verbose:
                print(f'Connecting to database {db_name} with host={os.environ["PGHOST"]}')            
            
            engine = sqlalchemy.create_engine( # type: ignore
               f'postgresql:///{db_name}',
               connect_args={"options": "-c timezone=utc"})

        # Patch engine with connection-oriented extensions from ConnectionExtensions
        for member_name, member in ConnectionExtensions.__dict__.items():
            if isinstance(member, types.FunctionType) and member_name[:2] != "__":
                setattr(self, member_name, functools.partial(_with_connect, self, member_name)) # type: ignore

        self.engine = engine

    def connect(self) -> ConnectionExtensions: # type: ignore
        con = self.engine.connect()

        # Patch connection with extensions from ConnectionExtensions
        for member_name, member in ConnectionExtensions.__dict__.items():
            if isinstance(member, types.FunctionType) and member_name[:2] != "__":
                setattr(con, member_name, types.MethodType(member, con))

        return con # type: ignore
    
    def geocode_batch(self, addresses: list[str], max_results: int = 1, nthreads: int = 10):
        print('RECOMMEND USING geocode_in_place instead (faster, writes directly to table)')
        mutex = threading.Lock() # type: ignore
        ret: list[None | list[dict[str, Any]]] = [None] * len(addresses)
        i = len(addresses)
        def geocode():
            with self.connect() as con:
                nonlocal i
                while True:
                    with mutex:
                        if i == 0:
                            return
                        i -= 1
                        mine = i
                    ret[mine] = con.geocode(addresses[mine], max_results)
        threads: list[utils.ThCall] = []
        try:
            for _ in range(nthreads):
                threads.append(utils.ThCall(geocode))
            for thread in threads:
                thread.join()
        finally:
            i = 0
            for thread in threads:
                thread.join()
        return ret
    
    def geocode_chunk_in_place(self, table_name: str, begin_idx: str|None = None, end_idx: str|None = None, idx_name: str = 'idx'):
        if begin_idx != None:
            condition = f'and {begin_idx} <= {idx_name} and {idx_name} <= {end_idx}'
        else:
            condition = ''

        temp_table_name = f"tmp_geocode_chunk_{binascii.b2a_hex(os.urandom(20)).decode()}"

        # To prevent long write locks on the destination table, first create a temporary table slowly with all the 
        # geocoding results, and then quickly update all the geocodes from the temporary table into the destination table
        self.execute(f"""
        create temporary table {temp_table_name} on commit drop as
            select idx, 
                    coalesce(g.rating, -1) as geocode_rating, 
                    pprint_addy(g.addy) as normalized_full_address, 
                    st_transform(g.geomout, 4326) as geom
            from (select idx, full_address
                    from {table_name}
                    where geocode_rating is null {condition}
                    order by idx) As a
                left join lateral
                    geocode(pagc_normalize_address(a.full_address),1) As g on true;
    
        update {table_name} As t
        set (geocode_rating, normalized_full_address, geom)
            = (g.geocode_rating, g.normalized_full_address, g.geom)
        from {temp_table_name} As g
        where t.idx = g.idx
        """)
        sys.stdout.write(f'[coded {begin_idx}:{end_idx}]')
        sys.stdout.flush()

    def geocode_in_place(self, table_name: str, idx_name: str = 'idx', chunk_size: int = 500, nthreads:int = 15):
        # Performance is around 400 geocodes per second, on hal21 with 15 threads
        min_idx = self.execute_returning_dicts(f'select min(idx) from {table_name}')[0]['min']
        max_idx = self.execute_returning_dicts(f'select max(idx) from {table_name}')[0]['max']
        print(f'geocode_in_place: {idx_name} ranges from {min_idx} to {max_idx}')
        print(max_idx)
        pool = utils.SimpleThreadPoolExecutor(nthreads)
        chunks = list(range(min_idx, max_idx + 1, chunk_size))
        print(f'Geocoding in {len(chunks)} chunks of size {chunk_size}')
        for chunk in chunks:
            pool.submit(self.geocode_chunk_in_place, table_name, chunk, min(chunk + chunk_size - 1, max_idx), idx_name)
        pool.shutdown()

class TempSchema:
    def __init__(self, engine: Engine, prefix: str = "", delete: bool = False, cascade: bool = False):
        self.engine = engine
        self.delete = delete
        self.cascade = cascade
        self.schema = f"{prefix}tmp{''.join(random.choices(string.ascii_lowercase + string.digits, k=20))}"
    def __enter__(self):
        self.engine.execute(f'CREATE SCHEMA IF NOT EXISTS {self.schema}')
        return self.schema
    def __exit__(self, exc_type, exc_value, traceback): # type: ignore
        if self.delete:
            self.engine.execute(f'DROP SCHEMA IF EXISTS {self.schema} {"CASCADE" if self.cascade else ""}')
        pass



# %%
