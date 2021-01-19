import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


spark = (SparkSession
        .builder
        .appName("Pyspark Upsert Example")
        .getOrCreate()
        )

# jsonData = """{
#   "create": {
#     "playlists": [
#       {
#         "id": "4",
#         "user_id": "1",
#         "song_ids": [
#           "8",
#           "32"
#         ]
#       },
#       {
#         "id": "2",
#         "user_id": "4",
#         "song_ids": [
#           "16",
#           "18",
#           "12"
#         ]
#       },
#       {
#         "id": "3",
#         "user_id": "5",
#         "song_ids": [
#           "7",
#           "22",
#           "23",
#           "6",
#           "12"
#         ]
#       }
#     ]
#   },
#   "delete": {
#     "playlists": [
#       {
#         "id": "1",
#         "user_id": "2",
#         "song_ids": [
#           "8",
#           "32"
#         ]
#       }
#     ]
#   },
#   "update": {
#     "playlists": [
#       {
#         "id": "1",
#         "user_id": "2",
#         "song_ids": [
#           "11"
#         ]
#       },
#       {
#         "id": "2",
#         "user_id": "3",
#         "song_ids": [
#           "13",
#           "14"
#         ]
#       },
#       {
#         "id": "3",
#         "user_id": "7",
#         "song_ids": [
#           "17",
#           "2"
#         ]
#       }
#     ]
#   }
# }"""
#
# df = spark.read.json(spark.sparkContext.parallelize([jsonData]))
# print(df.schema.json())
#schema = df.schema
#df.show()

try:
    with open('../schema/source_schema.json') as f:
        source_schema = StructType.fromJson(json.load(f))
except ValueError:  # includes simplejson.decoder.JSONDecodeError
    print ('Decoding JSON has failed')
    raise ValueError

print("Source mixtape")
df = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").schema(source_schema).json("hdfs://localhost:9000/user/anithasubramanian/inputs/mixtape.json")
#df = spark.read.schema(schema).from(path='hdfs://localhost:9000/user/anithasubramanian/inputs/mixtape.json')
df.show(truncate=False)

print("Source Users")
readUserDF = df.withColumn('Exp_Results',F.explode('users')).select('Exp_Results.*')
readUserDF.show(truncate=False)

print("Source Playlists")
readPlaylistsDF = df.withColumn('Exp_Results',F.explode('playlists')).select('Exp_Results.*')
readPlaylistsDF.show(truncate=False)

print("Source Songs")
readSongsDF = df.withColumn('Exp_Results',F.explode('songs')).select('Exp_Results.*')
readSongsDF.show(truncate=False)
readPlaylistsDF.createOrReplaceTempView("playlists")

# print("Source Users")
# readUser.createOrReplaceTempView("users")
# usersDF = spark.sql("SELECT * FROM users")
# usersDF.show()
#
# print("Source Playlists")
# readPlaylists.createOrReplaceTempView("playlists")
# playlistsDF = spark.sql("SELECT * FROM playlists")
# playlistsDF.show()
#
# print("Source Songs")
# readSongs.createOrReplaceTempView("songs")
# songsDF = spark.sql("SELECT * FROM songs")
# songsDF.show()

edit_schema = None
try:
    with open('../schema/edit_schema.json') as f:
        edit_schema = StructType.fromJson(json.load(f))
except ValueError:  # includes simplejson.decoder.JSONDecodeError
    print ('Decoding JSON has failed')
    raise ValueError

print("Edit mixtape")
df = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").schema(edit_schema).json("hdfs://localhost:9000/user/anithasubramanian/inputs/edit.json")
#df = spark.read.schema(schema).from(path='hdfs://localhost:9000/user/anithasubramanian/inputs/mixtape.json')
df.show(truncate=False)

print( "Insert playlists")
createPlaylistsDF = df.withColumn('Exp_Results',F.explode('create.playlists')).select('Exp_Results.*')
createPlaylistsDF.show(truncate=False)

# df_upsert = readPlaylistsDF.union(createPlaylistsDF)
# df_upsert.orderBy('id').show()

print( "Insert playlists Result")
createPlaylistsDF.join(readPlaylistsDF, createPlaylistsDF.id == readPlaylistsDF.id, 'leftanti').show()

print("Delete playlists")
deletePlaylistsDF = df.withColumn('Exp_Results',F.explode('delete.playlists')).select('Exp_Results.*')
deletePlaylistsDF.show(truncate=False)

print("Delete playlists Result")
deletePlaylistsDF.join(readPlaylistsDF, deletePlaylistsDF.id == readPlaylistsDF.id, 'leftsemi').show()

print("Update playlists")
updatePlaylistsDF = df.withColumn('Exp_Results',F.explode('update.playlists')).select('Exp_Results.*')
updatePlaylistsDF.show(truncate=False)

print("Update playlists Result")
updatePlaylistsDF.join(readPlaylistsDF, (updatePlaylistsDF.id == readPlaylistsDF.id) & (updatePlaylistsDF.user_id == readPlaylistsDF.user_id), 'inner').show()

