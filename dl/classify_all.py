import os
import json
from dl.utils import classify_dataset


def classify_all_profiles(folder_path):
    print(f"Analyzing files in: {folder_path}\n")

    for filename in os.listdir(folder_path):
        if not filename.lower().endswith(".json"):
            continue

        file_path = os.path.join(folder_path, filename)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                dp = json.load(f)
        except Exception as e:
            print(f"❌ Error: {e}")
            continue

        dataset_type = classify_dataset(dp)

        print(f"📄 File: {filename}")
        print(f"   → Type {dataset_type}")
        print()


if __name__ == "__main__":
    classify_all_profiles(r"C:\Users\tanfo\Desktop\Profiles")
