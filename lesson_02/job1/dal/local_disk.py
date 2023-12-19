import json
import os
from typing import List, Dict, Any


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:

    if json_content is None:
        raise TypeError("json_content cannot be None")

    directory = os.path.dirname(path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, 'w', encoding='utf-8') as file:
        json.dump(json_content, file)

    pass
