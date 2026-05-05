# etl_framework/target_queries.py

def target_aggregates_query():
    return """
        SELECT 
            COUNT(*) AS total_rows,
            SUM(total_amount) AS total_revenue,
            MAX(total_amount) AS max_trip,
            MIN(total_amount) AS min_trip,
            AVG(total_amount) AS avg_trip
        FROM YELLOW_TRIPDATA_TEST
    """


def target_groupby_query():
    return """
        SELECT payment_type, COUNT(*) AS cnt
        FROM YELLOW_TRIPDATA_TEST
        GROUP BY payment_type
    """


def target_filter_query():
    return """
        SELECT COUNT(*) 
        FROM YELLOW_TRIPDATA_TEST
        WHERE passenger_count > 4
    """