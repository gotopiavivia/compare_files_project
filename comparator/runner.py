import sys
from comparator.service import compare_files


def run_comparison():
    if len(sys.argv) != 5:
        print(
            "Usage:\n"
            "python main.py <file1.csv> <file2.csv> <key_column> <output.csv>"
        )
        sys.exit(1)

    file1, file2, key_column, output_file = sys.argv[1:]

    try:
        compare_files(file1, file2, key_column, output_file)
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
