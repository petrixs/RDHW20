"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
from flask import Flask, request
from flask import typing as flask_typing

from lesson_02.job2.bll.sales_api import save_sales_to_local_disk


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:

    input_data: dict = request.json
    stg_dir = input_data.get('stg_dir')
    raw_dir = input_data.get('raw_dir')

    if not stg_dir:
        return {"message": "stg_dir parameter is missing"}, 400

    if not raw_dir:
        return {"message": "raw_dir parameter is missing"}, 400

    if not os.path.exists(raw_dir):
        return {"message": "raw_dir does not exist"}, 400

    save_sales_to_local_disk(stg_dir=stg_dir, raw_dir=raw_dir)

    return {
               "message": "Data successfully saved to stg dir",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
