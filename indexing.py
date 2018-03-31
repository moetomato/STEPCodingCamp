import os
import wp

try:
    # 過去のindexは削除する
    os.remove("data/index.db")
except OSError:
    pass
collection = wp.WikipediaCollection("data/wp.db")
index = wp.Index("data/index.db", collection)
index.generate()
