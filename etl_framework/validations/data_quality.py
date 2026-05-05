def validate_nulls(df_source, df_target):

    return {
        "source_nulls": df_source.isnull().sum().to_dict(),
        "target_nulls": df_target.isnull().sum().to_dict()
    }


def validate_duplicates(df_source, df_target):

    return {
        "source_duplicates": int(df_source.duplicated().sum()),
        "target_duplicates": int(df_target.duplicated().sum())
    }