# Databricks notebook source
def insert_log(result):
    try:
        if not result:
            raise Exception("No result was sent for logging!")

        param_id = result["PARAM_ID"]
        query = result["QUERY"].replace("'", "''")
        result = result["RESULT"]

        spark.sql(
            f"""
                INSERT INTO framework_testes.logs
                (PARAM_ID, QUERY, RESULT, TIMESTAMP)
                VALUES (
                    {param_id},
                    "{query}",
                    "{result}",
                    current_timestamp()
                )
            """
        )

        print("[!] Logged entry...")
    except Exception as e:
        print(f"[!] Caught exception in insert_log: {e}")
    return 