from IESOReports import Report
from sqlalchemy import create_engine, text
from prefect import flow, tags, task
from prefect.schedules import Interval
from datetime import timedelta, datetime
import psycopg2, textwrap, os
import polars as pl, pandas as pd


@task(cache_policy=None)
def getPublicReportList(engine,schema):
    query = f"""\
    SELECT * 
    FROM {schema}.public_reports
    WHERE is_legacy IS NULL OR is_legacy = false;
    ;
    """
    
    result = pd.read_sql(query, engine)
        
    return result

@task(cache_policy=None)
def cloneTable(schema,tablename):
    query = f"""\
    DROP TABLE IF EXISTS temp_{tablename.lower()};
    CREATE TEMP TABLE temp_{tablename.lower()} AS
    SELECT * FROM {schema.lower()}.{tablename.lower()}
    LIMIT 0
    ;
    """
            
    return textwrap.dedent(query)

@task(cache_policy=None)
def getPK(engine,schema,tablename):
    # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    query = f"""\
    SELECT a.attname
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                         AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = '{schema.lower()}.{tablename.lower()}'::regclass
    AND    i.indisprimary
    ;
    """
        
    result = []
    
    for r in engine.connect().execute(text(query)):
        result.append(r[0])
        
    return result

@task(cache_policy=None)
def getFields(engine,schema,tablename):
    query = f"""\
    SELECT	column_name
    FROM	information_schema.columns
    WHERE
    	table_schema = '{schema.lower()}'
    	AND table_name = '{tablename.lower()}'
    ;
    """
    
    result = []
    
    for r in engine.connect().execute(text(query)):
        result.append(r[0])
        
    return result

@task(cache_policy=None)
def upsert(schema,tablename,fields,pkeys):   
    update_fields = [x for x in fields if x not in pkeys]
    do_update_set = []
    
    for f in update_fields:    
        line = f"{f} = EXCLUDED.{f}"
        do_update_set.append(line)
    
    do_update_set = ', '.join(do_update_set)
    
    query = f"""\
    INSERT INTO {schema.lower()}.{tablename.lower()}
    SELECT {",".join(fields)}
    FROM temp_{tablename.lower()}
    ON CONFLICT ({",".join(pkeys)}) 
    DO UPDATE SET
        {do_update_set}
    ;
    """

    return textwrap.dedent(query)


@task(cache_policy=None)
def massSend(engine,schema,report):

    folder = r"\\NAS\WD Passport\reports-public.ieso.ca\public"
                    
    # get files with snazzy list comprehension
    allfiles = [os.path.join(folder,report,f) for f in os.listdir(os.path.join(folder,report)) if os.path.isfile(os.path.join(folder,report,f))]
    
    fields = getFields(engine,schema,report.lower())
    pk = getPK(engine,schema,report.lower())
    print(f"successfully retrieved fields and pkeys")
    
    with engine.begin() as conn:
        # clone main table as a temp table
        conn.execute(text(cloneTable(schema,report.lower())))
        
        for i,f in enumerate(reversed(allfiles)):
            
            r = Report(f)
            
            # append each report file to the new temp table in the db
            # I could create a giant dataframe and send it to the db all at once
            # but it chokes smaller servers - I tried it and I do not recommend
            if r.isValid and r.version is None:
                print(f"sending {f} to temp_{r.name.lower()}")
                df = r.parse()
                df = df.to_pandas()
                
                df.to_sql(
                    name=f'temp_{r.name.lower()}',
                    con=conn,
                    if_exists="append",
                    index=False,
                    method='multi',
                )
                                    
        # once done writing to temp table upsert with the main table
        print(f"upserting to {schema}.{r.name.lower()}")
        conn.execute(text(upsert(schema,r.name,fields,pk)))

@flow(log_prints=True)        
def IESODatabase_deploy():
    from IESOConfig import hostname, database, username, password, schema, mainfolder as folder
    report = ["RealtimeEnergyLMP","DAHourlyEnergyLMP"]
    
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}/{database}',executemany_mode="values_plus_batch")    
    
    for r in report:
        massSend(engine,schema,r)
        
if __name__ == "__main__":
            
    IESODatabase_deploy()
        
