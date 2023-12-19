import os
import shutil
from lesson_02.job1.dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:

    # Просто видаляти усі директорії в raw_dir небезпечно, тому будемо видаляти тількі sales,
    # це допоможе не видалити потрібні файли якщо хтось помилиться в змінній raw_dir
    sales_dir = os.path.join(raw_dir, 'sales')

    if os.path.exists(sales_dir):
        shutil.rmtree(sales_dir)

    file_name = os.path.join(raw_dir, f"sales/sales_{date}.json")

    sales_data = sales_api.get_sales(date)
    local_disk.save_to_disk(sales_data, file_name)

    print("\tI'm in get_sales(...) function!")
    pass
