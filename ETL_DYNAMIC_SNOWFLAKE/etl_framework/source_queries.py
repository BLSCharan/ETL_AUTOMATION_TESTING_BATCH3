# etl_framework/source_queries.py

SOURCE_LIMIT = 1000000


def source_aggregates_query():
    return f"""
        SELECT 
            COUNT(*) AS total_rows,
            SUM(CAST(total_amount AS FLOAT)) AS total_revenue,
            MAX(CAST(total_amount AS FLOAT)) AS max_trip,
            MIN(CAST(total_amount AS FLOAT)) AS min_trip,
            AVG(CAST(total_amount AS FLOAT)) AS avg_trip
        FROM (
            SELECT TOP {SOURCE_LIMIT} *
            FROM yellow_tripdata_2015_02
        ) t
    """


def source_groupby_query():
    return f"""
        SELECT payment_type, COUNT(*) AS cnt
        FROM (
            SELECT TOP {SOURCE_LIMIT} *
            FROM yellow_tripdata_2015_02
        ) t
        GROUP BY payment_type
    """


def source_filter_query():
    return f"""
        SELECT COUNT(*) 
        FROM (
            SELECT TOP {SOURCE_LIMIT} *
            FROM yellow_tripdata_2015_02
        ) t
        WHERE passenger_count > 4
    """