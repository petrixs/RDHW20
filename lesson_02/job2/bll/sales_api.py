import os
from lesson_02.job2.dal import local_disk
import json


def save_sales_to_local_disk(stg_dir: str, raw_dir: str) -> None:

    for root, dirs, files in os.walk(raw_dir):
        for file in files:
            if file.endswith('.json'):

                json_file_path = os.path.join(root, file)

                with open(json_file_path, 'r') as json_file:
                    json_data = json.load(json_file)

                relative_path = os.path.relpath(root, raw_dir)
                stg_file_dir = os.path.join(stg_dir, relative_path)

                if not os.path.exists(stg_file_dir):
                    os.makedirs(stg_file_dir)

                avro_file_name = f"{os.path.splitext(file)[0]}.avro"
                avro_file_path = os.path.join(stg_file_dir, avro_file_name)

                # Save the JSON data as an Avro file in the stg_dir
                local_disk.save_to_disk(json_data, avro_file_path)

    print("\tI'm in get_sales(...) function!")
    pass
