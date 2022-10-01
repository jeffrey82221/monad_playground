"""
Calling etl_api at fastapi_playground deployed on my mac mini
"""
from functools import wraps
import requests
import inspect
from textwrap import dedent
import configparser
from typing import List, Dict, Callable
import json
from pathlib import Path
import os
config_path = os.path.join(Path(__file__).parent, 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)
URL = config.get('remote', 'url')


def call_etl_api(class_func: bool, orig_func: Callable,
                 arg_names: List[str], kwarg_names: Dict[str, str], output_names: List[str]):
    python_func = dedent(inspect.getsource(orig_func))
    data = {
        'class_func': class_func,
        'python_func': python_func,
        'func_name': orig_func.__name__,
        'arg_names': arg_names,
        'kwarg_names': kwarg_names,
        'output_names': output_names
    }
    response = requests.post(
        URL,
        json=data
    )

    assert response.status_code == 200, f'status code is {response.status_code}'
    assert response.json()['success'], response.json()['msg']
    print(response.json())


def generate_result():
    if True:
        return 'hello_world'
    else:
        pass


if __name__ == '__main__':
    call_etl_api(
        False,
        generate_result,
        arg_names=[],
        kwarg_names=dict(),
        output_names=['hello_str'])
