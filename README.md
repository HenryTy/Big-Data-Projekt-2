# Big-Data-Projekt-2

### Utworzenie tabel hurtowni:
```
spark-shell -i hurtownia.scala
```

### Załadowanie danych do tabel
```
Dane muszą znajdować się w HDFS w labs/spark/uk-traffic
```
```
spark-submit --class com.example.bigdata.Czas \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 czas.jar labs/spark/uk-traffic

spark-submit --class com.example.bigdata.PogodaETL \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 pogoda-etl.jar labs/spark/uk-traffic

spark-submit --class com.example.bigdata.Typy \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 typy.jar labs/spark/uk-traffic

spark-submit --class com.example.bigdata.Miejsca \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 miejsca.jar labs/spark/uk-traffic

spark-submit --class com.example.bigdata.Facts \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 fakty.jar labs/spark/uk-traffic
```
