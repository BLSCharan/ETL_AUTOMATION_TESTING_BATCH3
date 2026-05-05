def validate_columns(df_source, df_target):

    source_cols = set(col.lower() for col in df_source.columns)
    target_cols = set(col.lower() for col in df_target.columns)

    if source_cols == target_cols:
        return "PASS"
    else:
        return {
            "Missing_in_Target": list(source_cols - target_cols),
            "Extra_in_Target": list(target_cols - source_cols)
        }