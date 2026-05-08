import argparse
import gzip
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    args = parser.parse_args()

    base_path = Path(args.path)

    files = sorted(base_path.glob("*.json.gz"))
    if not files:
        raise FileNotFoundError(f"No .json.gz files found in {base_path}")

    total = 0

    for file_path in files:
        with gzip.open(file_path, "rt", encoding="utf-8") as file:
            for _ in file:
                total += 1

    print(total)


if __name__ == "__main__":
    main()