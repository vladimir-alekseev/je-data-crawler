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
```
git clone https://github.com/vladimir-alekseev/je-data-crawler.git
cd je-data-crawler
pip install .
```
Docker-ready:
```
docker pull vladimiralekseev/je-data-crawler
```
Dependencies:
* python >= 3.7
* [requests](https://requests.readthedocs.io/)
* [mysql-connector-python](https://dev.mysql.com/downloads/connector/python/)

## Use
Tune `crawler.ini` to your needs and run:

    data_crawler
Use `-c` argument for custom config file path:

    data_crawler -c ../configs/config.ini

Docker-friendly:

    docker run -v /path/to/crawler.ini:/config.ini -v /path/to/backup.csv:/backup.csv vladimiralekseev/je-data-crawler
Also check out [docker-compose.yml](https://github.com/vladimir-alekseev/je-data-crawler/tree/master/docker-compose.yml) if you really fancy.
## Questions?
E-mail: vladimir.alekseev@gmail.com  
LinkedIn: https://www.linkedin.com/in/vladimiralekseyev