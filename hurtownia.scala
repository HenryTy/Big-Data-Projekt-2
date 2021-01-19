spark.sql("DROP TABLE IF EXISTS `czas`")
spark.sql("DROP TABLE IF EXISTS `typy_pojazdow`")
spark.sql("DROP TABLE IF EXISTS `miejsca`")
spark.sql("DROP TABLE IF EXISTS `pogoda`")
spark.sql("DROP TABLE IF EXISTS `fakty`")

spark.sql("""CREATE TABLE `czas` (
 `id` bigint,
 `rok` int,
 `miesiac` int,
 `data` date,
 `godzina` int,
 `kwartal` int,
 `dzien_tygodnia` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `typy_pojazdow` (
 `id` int,
 `typ` string,
 `kategoria` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `miejsca` (
 `id` bigint,
 `kod_ons_obszaru` string,
 `nazwa_obszaru` string,
 `nazwa_regionu` string,
 `nazwa_drogi` string,
 `kategoria_drogi` string,
 `typ_drogi` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `pogoda` (
 `id` int,
 `opis_pogody` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `fakty` (
 `id_czasu` bigint,
 `id_pojazdu` int,
 `id_miejsca` bigint,
 `id_pogody` int,
 `liczba_pojazdow` int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")
