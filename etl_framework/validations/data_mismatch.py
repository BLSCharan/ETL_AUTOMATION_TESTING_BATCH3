def validate_data_mismatch(df_source, df_target, primary_key):

    try:

        df_source.columns = df_source.columns.str.lower()
        df_target.columns = df_target.columns.str.lower()
        primary_key = primary_key.lower()

        for col in df_source.select_dtypes(include=["object", "string"]).columns:
            df_source[col] = df_source[col].str.strip()
            df_target[col] = df_target[col].str.strip()

        merged = df_source.merge(
            df_target,
            on=primary_key,
            how="outer",
            suffixes=("_source", "_target"),
            indicator=True
        )

        missing_in_target = merged[merged["_merge"] == "left_only"]
        missing_in_source = merged[merged["_merge"] == "right_only"]

        column_mismatches = []

        common_records = merged[merged["_merge"] == "both"]

        for col in df_source.columns:

            if col == primary_key:
                continue

            source_col = f"{col}_source"
            target_col = f"{col}_target"

            if source_col not in common_records.columns:
                continue

            diff = common_records[
                ~(common_records[source_col].fillna("NULL").astype(str)
                  ==
                  common_records[target_col].fillna("NULL").astype(str))
            ]

            if not diff.empty:
                column_mismatches.append(col)

        return {
            "missing_in_target": len(missing_in_target),
            "missing_in_source": len(missing_in_source),
            "column_mismatches": column_mismatches
        }

    except Exception as e:
        return f"Error: {str(e)}"