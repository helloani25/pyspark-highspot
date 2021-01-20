import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import struct
from pyspark.sql.types import StructType, Row

spark = (SparkSession
         .builder
         .appName("Pyspark Upsert Example")
         .getOrCreate()
         )

try:
    with open('../schema/source_schema.json') as f:
        source_schema = StructType.fromJson(json.load(f))
except ValueError:  # includes simplejson.decoder.JSONDecodeError
    print('Decoding JSON has failed')
    raise ValueError

print("Source mixtape")
df = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").schema(source_schema).json(
    "hdfs://localhost:9000/user/anithasubramanian/inputs/mixtape.json")
df.show(truncate=False)

print("Source Users")
readUserDF = df.withColumn('Exp_Results', F.explode('users')).select('Exp_Results.*')
readUserDF.show(truncate=False)

print("Source Playlists")
readPlaylistsDF = df.withColumn('Exp_Results', F.explode('playlists')).select('Exp_Results.*')
readPlaylistsDF.show(truncate=False)

print("Source Songs")
readSongsDF = df.withColumn('Exp_Results', F.explode('songs')).select('Exp_Results.*')
readSongsDF.show(truncate=False)
readPlaylistsDF.createOrReplaceTempView("playlists")

edit_schema = None
try:
    with open('../schema/edit_schema.json') as f:
        edit_schema = StructType.fromJson(json.load(f))
except ValueError:  # includes simplejson.decoder.JSONDecodeError
    print('Decoding JSON has failed')
    raise ValueError

print("Edit mixtape")
df_edit = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").schema(edit_schema).json(
    "hdfs://localhost:9000/user/anithasubramanian/inputs/edit.json")
df_edit.show(truncate=False)

songs = readSongsDF.select("id").rdd.flatMap(lambda x: x).collect()
readSongsDF.unpersist()

print("Insert playlists")
createPlaylistsDF = df_edit.withColumn('Exp_Results', F.explode('create.playlists')).select('Exp_Results.*')
createPlaylistsDF.show(truncate=False)

print("Insert playlists Result")
createPlaylistsDF = createPlaylistsDF.join(readPlaylistsDF, createPlaylistsDF.id == readPlaylistsDF.id, 'leftanti').join(readUserDF,
                                                                                                     createPlaylistsDF.user_id == readUserDF.id,
                                                                                                     'inner').select(
    createPlaylistsDF.id, F.array_intersect(createPlaylistsDF.song_ids, F.array([F.lit(x) for x in songs])).alias("song_ids"), createPlaylistsDF.user_id)
readPlaylistsDF = readPlaylistsDF.union(createPlaylistsDF)
createPlaylistsDF.unpersist()
readUserDF.unpersist()

readPlaylistsDF.orderBy('id').show()

print("Delete playlists")
deletePlaylistsDF = df_edit.withColumn('id', F.explode('delete.playlist_ids')).select("id")
deletePlaylistsDF.show(truncate=False)

print("Delete playlists Result")
readPlaylistsDF = readPlaylistsDF.join(deletePlaylistsDF, readPlaylistsDF.id == deletePlaylistsDF.id, "left_outer").where(deletePlaylistsDF.id.isNull()).select(readPlaylistsDF.id, readPlaylistsDF.song_ids, readPlaylistsDF.user_id)
readPlaylistsDF.show()
deletePlaylistsDF.unpersist()

print("Update playlists")
updatePlaylistsDF = df_edit.withColumn('Exp_Results', F.explode('update.playlists')).select('Exp_Results.*')
updatePlaylistsDF.show(truncate=False)

print("Update playlists Result")
updatePlaylistsDF = updatePlaylistsDF.join(readPlaylistsDF, (updatePlaylistsDF.id == readPlaylistsDF.id) & (
        updatePlaylistsDF.user_id == readPlaylistsDF.user_id), 'inner').select(updatePlaylistsDF.id,
                                                                               updatePlaylistsDF.user_id,
                                                                               F.array_union(F.array_intersect(
                                                                                   updatePlaylistsDF.song_ids,
                                                                                   F.array([F.lit(x) for x in songs])),
                                                                                   readPlaylistsDF.song_ids).alias(
                                                                                   "song_ids"))

readPlaylistsDF = readPlaylistsDF.join(updatePlaylistsDF, readPlaylistsDF.id == updatePlaylistsDF.id, "left").select(readPlaylistsDF.id, F.coalesce(updatePlaylistsDF.song_ids, readPlaylistsDF.song_ids).alias("song_ids"), readPlaylistsDF.user_id)
readPlaylistsDF.show()

updatePlaylistsDF.unpersist()

playlistsDF = readPlaylistsDF.agg(F.collect_list(struct("*")).alias('playlists'))
readPlaylistsDF.unpersist()
result = df

result = result.drop("playlists")
result = result.crossJoin(playlistsDF)
playlistsDF.unpersist()
result.coalesce(1).write.format('json').save('hdfs://localhost:9000/user/anithasubramanian/outputs/mixtape4.json')


result.show(truncate=False)




