import pandas as pd
from comparator.io_utils import load_csv


OUTPUT_COLUMNS = [
    "id",
    "column_name",
    "file1_value",
    "file2_value",
    "difference_type",
]


def compare_files(file1: str, file2: str, key_column: str, output_file: str) -> None:
    # Load files
    df1 = load_csv(file1)
    df2 = load_csv(file2)

    if key_column not in df1.columns or key_column not in df2.columns:
        raise ValueError(f"Key column '{key_column}' must exist in both files")

    # Set index
    df1 = df1.set_index(key_column)
    df2 = df2.set_index(key_column)

    # -----------------------------
    # 1. Detect missing rows
    # -----------------------------
    keys_file1 = set(df1.index)
    keys_file2 = set(df2.index)

    only_in_file1 = keys_file1 - keys_file2
    only_in_file2 = keys_file2 - keys_file1

    missing_records = []

    for key in only_in_file1:
        for col in df1.columns:
            missing_records.append({
                key_column: key,
                "column_name": col,
                "file1_value": df1.at[key, col],
                "file2_value": None,
                "difference_type": "ROW_MISSING"
            })

    for key in only_in_file2:
        for col in df2.columns:
            missing_records.append({
                key_column: key,
                "column_name": col,
                "file1_value": None,
                "file2_value": df2.at[key, col],
                "difference_type": "ROW_MISSING"
            })

    missing_df = pd.DataFrame(missing_records, columns=OUTPUT_COLUMNS)

    # -----------------------------
    # 2. Compare only common rows
    # -----------------------------
    common_keys = sorted(keys_file1 & keys_file2)

    df1_common = df1.loc[common_keys]
    df2_common = df2.loc[common_keys]

    diff_df = df1_common.compare(
        df2_common,
        keep_equal=False,
        result_names=("file1_value", "file2_value")
    )

    if diff_df.empty:
        value_diff_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        # Flatten MultiIndex columns
        diff_df = diff_df.stack(level=0).reset_index()
        diff_df.columns = [
            key_column,
            "column_name",
            "file1_value",
            "file2_value",
        ]

        diff_df["difference_type"] = "VALUE_MISMATCH"
        value_diff_df = diff_df[OUTPUT_COLUMNS]

    # -----------------------------
    # 3. Final output
    # -----------------------------
    final_df = pd.concat(
        [value_diff_df, missing_df],
        ignore_index=True
    ).sort_values(
        by=[key_column, "difference_type", "column_name"]
    )

    final_df.to_csv(output_file, index=False)

    print("✔ Comparison completed successfully")
    print(f"✔ Output written to: {output_file}")
    print(f"✔ Total differences found: {len(final_df)}")
