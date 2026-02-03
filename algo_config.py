TEST_ITEM_CONFIG = {
    "capacity": {
        "strategy": "CapacityStrategy",
        "script": "capacity.py",
        "query_sql": """
            SELECT order_no, cell_id, value
            FROM raw_test_data
            WHERE order_no='{order_no}'
              AND cell_id='{cell_id}'
        """,
        "insert_table": "algo_result",
        "insert_cols": ["order_no", "cell_id", "result"]
    },

    "RESISTANCE": {
        "strategy": "ResistanceStrategy",
        "script": "resistance.py",
        "query_sql": """
            SELECT voltage, current
            FROM raw_test_data
            WHERE order_no='{order_no}'
              AND cell_id='{cell_id}'
        """,
        "insert_table": "algo_resistance_result",
        "insert_cols": ["order_no", "cell_id", "resistance"]
    }
}
