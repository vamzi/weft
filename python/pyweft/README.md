# pyweft

Launches a [Weft](https://gitlab.com/weftlabs/weft) Spark Connect server. Bring your own
stock PySpark client and change one line:

```python
from pyweft import SparkConnectServer
SparkConnectServer(port=50051).start()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT count(*) FROM parquet.`hits.parquet`").show()
```

```sh
pip install "pyweft[client]"
```
