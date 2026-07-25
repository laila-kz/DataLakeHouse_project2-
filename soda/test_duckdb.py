import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL delta; LOAD delta;")
con.execute("SET s3_endpoint='minio:9000';")
con.execute("SET s3_access_key_id='minioadmin';")
con.execute("SET s3_secret_access_key='minioadmin';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")

res = con.execute("SELECT count(*) FROM delta_scan('s3://bronze/ecommerce_events/')").fetchall()
print("DUCKDB ROW COUNT:", res)
