# Data crawler for Jobs Explorer

Collect jobs data from various sources and store it on supported data storages.

Supported sources:
* [HH.ru API](https://dev.hh.ru/)
* CSV files

Supported storages:
* MariaDB
* CSV files
* Raw log output

## Install
1. Get source code using `git` and install manually
```
git clone https://github.com/vladimir-alekseev/je_data_crawler.git
cd je_data_crawler
python setup.py install
```
2. Download freshly backed .whl file from [dist/](https://github.com/vladimir-alekseev/je-data-crawler/tree/master/dist) and `pip install` as usual.

Dependencies:
* python >= 3.7
* [requests](https://requests.readthedocs.io/)
* [mysql-connector-python](https://dev.mysql.com/downloads/connector/python/)

## Use
Tune `crawler.ini` to your needs and run:

    data_crawler
Use `-c` argument for custom config file path:

    data_cralwer -c ../configs/config_file.ini

## Questions?
E-mail: vladimir.alekseev@gmail.com  
LinkedIn: https://www.linkedin.com/in/vladimiralekseyev