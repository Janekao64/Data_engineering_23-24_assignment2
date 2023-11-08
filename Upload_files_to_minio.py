#!/usr/bin/env python
# coding: utf-8

# In[1]:


from minio import Minio
from minio.error import S3Error

minio_client = Minio(
    "127.0.0.1:9000", 
    access_key="5QXDQgNpRpaQGoZnIqTO",
    secret_key="6ciZe6CEapuhXloljqY8CtutN1Y1gd6zwRfscOKm",
    secure=False  # You can set it to True if you use HTTPS
)

found = minio_client.bucket_exists("assignment2")
if not found:
    minio_client.make_bucket("assignment2")
else:
    print("Bucket 'assignment2' already exists")


# In[3]:


import os 
source_folder = "C:/Users/90545/Documents/Data engineering/Assignment 2/my_parquet_data"

minio_bucket = "assignment2"

for root, dirs, files in os.walk(source_folder):
    for file in files:
        file_path = os.path.join(root, file)
        object_name = os.path.relpath(file_path, source_folder) 

        try:
            minio_client.fput_object(minio_bucket, object_name, file_path)
            print(f"Uploaded: {object_name}")
        except S3Error as e:
            print(f"Error uploading {object_name}: {e}")


# In[ ]:




