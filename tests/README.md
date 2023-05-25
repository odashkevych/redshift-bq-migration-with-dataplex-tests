# DQ Tests

DQ dimensions https://www.datagaps.com/blog/what-are-data-quality-dimensions/

Compile generic tests and deploy to DQ bucket.

Events
```shell
python compile.py models/event.yaml compiled dev && ./copy_rules.sh compiled/models
```

Sales
```shell
python compile.py models/sales.yaml compiled dev && ./copy_rules.sh compiled/models/sales.yaml
```

Users
```shell
python compile.py models/users.yaml compiled dev && ./copy_rules.sh compiled/models
```
