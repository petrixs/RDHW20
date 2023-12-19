import os
import requests
from typing import List, Dict, Any

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")


def get_sales(date: str) -> List[Dict[str, Any]]:

    sales_data = []
    page = 1
    while True:
        response = requests.get(
            f"{API_URL}sales",
            params={"date": date, "page": page},
            headers={"Authorization": AUTH_TOKEN}
        )

        if response.status_code != 200:
            break

        data = response.json()
        if not data:
            break

        sales_data.extend(data)
        page += 1

    return sales_data

