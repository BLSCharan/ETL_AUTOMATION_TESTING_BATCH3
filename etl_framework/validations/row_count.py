def validate_row_count(df_source, df_target):

    source_count = len(df_source)
    target_count = len(df_target)

    if source_count == target_count:
        return f"PASS - Source: {source_count}, Target: {target_count}"
    else:
        return f"FAIL - Source: {source_count}, Target: {target_count}"