# Databricks notebook source
# MAGIC %md
# MAGIC # Crawl data from the google image

# COMMAND ----------

# MAGIC %md
# MAGIC Make the dataset connected into Azure
# MAGIC

# COMMAND ----------

# %pip install beautifulsoup4
# %pip install openpyxl
# !pip install selenium pillow
# !pip install webdriver_manager


# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# import package
import pandas as pd
from bs4 import BeautifulSoup
import requests

# COMMAND ----------

ethnic_df = pd.read_excel("Datasets/Ethnic_Origin_Facial_Issues.xlsx")
issue_df = pd.read_excel("Datasets/Frequent_Facial_Issues.xlsx")

# COMMAND ----------

# Add a constant key
ethnic_df['key'] = 1
issue_df['key'] = 1

# Perform cross join
cross_joined_df = pd.merge(ethnic_df, issue_df, on='key').drop('key', axis=1)

# COMMAND ----------

cross_joined_df.head()

# COMMAND ----------

cross_joined_df.shape

# COMMAND ----------

## Why this cannot work
# spark.write\
# .table("cross_joined_df")\
# .saveAsTable("cross_joined_df")


# COMMAND ----------

col_name  = list(cross_joined_df.columns)
col_name

# COMMAND ----------

df =  cross_joined_df[['Ethnic Origin',
 'Issue']]
df.head()

# COMMAND ----------

df["key_word"] = df["Ethnic Origin"]+"'s "+df["Issue"]
df.head()

# COMMAND ----------

for index, row in df.iterrows():
    print(row["key_word"])

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import os
os.mkdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM/images")
os.chdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM/images")
print(os.getcwd())

for index, row in df.iterrows():
    search_query = row["key_word"]

    # Construct the URL for Google Images
    url = f"https://www.google.com/search?hl=en&tbm=isch&q={search_query}"

    # Perform the HTTP request
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find image URLs
    images = soup.find_all('img')
    image_urls = [img['src'] for img in images if img['src'].startswith('http')]

    # Create directory for images if it doesn't exist
    if not os.path.exists(row["Issue"]):
        os.makedirs(search_query)

    # Download the first 10 high-quality images
    count = 0
    for img_url in image_urls:
        if count >= 10:
            break
        try:
            img_data = requests.get(img_url).content
            img_size = len(img_data)
            if img_size > 50000:  # Filter for high-quality images (size > 50KB)
                with open(f"{search_query}/image_{count+1}.jpg", 'wb') as f:
                    f.write(img_data)
                print(f"Downloaded {search_query}/image_{count+1}.jpg")
                count += 1
        except Exception as e:
            print(f"Failed to download image: {e}")

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import os
import mimetypes
os.mkdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM/image")
os.chdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM/image")
print(os.getcwd())


for index, row in df.iterrows():
    search_query = row["key_word"]

    # Construct the URL for Google Images
    url = f"https://www.google.com/search?hl=en&tbm=isch&q={search_query}"

    # Perform the HTTP request
    response = requests.get(url)

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find image URLs
    images = soup.find_all('img')
    image_urls = [img['src'] for img in images if img['src'].startswith('http')]

    # Create directory for images if it doesn't exist
    if not os.path.exists(row["Issue"]):
        os.makedirs(row["Issue"])

    # Download the first 10 images
    for idx, img_url in enumerate(image_urls[:100]):
        try:
            img_data = requests.get(img_url).content
            file_ext = mimetypes.guess_extension(requests.head(img_url).headers.get('Content-Type', '').split(';')[0])

            if not file_ext:  # 如果没有明确的文件扩展名，则默认为 '.jpg'
                    file_ext = '.jpg'
                    print(f"Failed to guess file extension for {img_url}, using default '.jpg'")
            img_filename = f'{row["Issue"]}/{row["Issue"]}_{idx+1}{file_ext}' # You can change the files name here 
            with open(img_filename, 'wb') as f:
                f.write(img_data)
            print(f"Downloaded search_query_{idx+1}.jpg")
        except Exception as e:
            print(f"Failed to download image_{idx+1}: {e}")

# COMMAND ----------

# import requests
# from bs4 import BeautifulSoup
# import os
# import mimetypes
# # Path
# os.mkdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM/image_high") # If you want to change the location, only change image_high and for up and below
# os.chdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM/image_high")
# print(os.getcwd())


# for index, row in df.iterrows():
#     search_query = row["key_word"]

#     # Construct the URL for Google Images
#     url = f"https://www.google.com/search?hl=en&tbm=isch&q={search_query}"

#     # Perform the HTTP request
#     response = requests.get(url)

#     # Parse the HTML content
#     soup = BeautifulSoup(response.text, 'html.parser')

#     # Find image URLs
#     images = soup.find_all('img')
#     image_urls = [img['src'] for img in images if img['src'].startswith('http')]

#     # Create directory for images if it doesn't exist
#     if not os.path.exists(row["Issue"]):
#         os.makedirs(row["Issue"])

#     # Download the first 10 images
#     count = 0
#     for idx, img_url in enumerate(image_urls):
#       if count>10:
#         break
#         try:
#             img_data = requests.get(img_url).content
#             if len(img_data)>3000:
#               file_ext = mimetypes.guess_extension(requests.head(img_url).headers.get('Content-Type', '').split(';')[0])

#               if not file_ext:  # 如果没有明确的文件扩展名，则默认为 '.jpg'
#                       file_ext = '.jpg'
#                       print(f"Failed to guess file extension for {img_url}, using default '.jpg'")
#               img_filename = f'{row["Issue"]}/{row["Issue"]}_{idx+1}{file_ext}'
#               with open(img_filename, 'wb') as f:
#                   f.write(img_data)
#               print(f"Downloaded search_query_{idx+1}.jpg")
#               count+=1
#         except Exception as e:
#             print(f"Failed to download image_{idx+1}: {e}")

# COMMAND ----------

# import requests
# from bs4 import BeautifulSoup
# import os
# from urllib.parse import urljoin

# os.chdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM")
# print(os.getcwd())
# if not os.path.exists("./image"):
#   os.mkdir("/Workspace/Users/hanchen@845861076.onmicrosoft.com/Dermatology_LLM/image")

# # 逐行遍历 DataFrame
# for index, row in df.iterrows():
#     search_query = row["key_word"]
#     issue_folder = row["Issue"].replace(" ", "_")  # 确保文件夹名没有空格等非法字符

#     # 创建目录
#     if not os.path.exists(issue_folder):
#         os.makedirs(issue_folder)

#     # 构建 Google 图片搜索的 URL
#     url = f"https://www.google.com/search?hl=en&tbm=isch&q={search_query}"

#     # 执行 HTTP 请求
#     response = requests.get(url)

#     # 检查请求是否成功
#     if response.status_code != 200:
#         print(f"Failed to retrieve images for {search_query}")
#         continue

#     # 解析 HTML 内容
#     soup = BeautifulSoup(response.text, 'html.parser')

#     # 查找所有图片的 URL
#     images = soup.find_all('img')
#     image_urls = [urljoin(url, img['src']) for img in images if img.get('src') and img['src'].startswith('http')]

#     # 下载前 10 张高质量图片
#     count = 0
#     for img_url in image_urls:
#         if count >= 10:
#             break
#         try:
#             img_data = requests.get(img_url).content
#             img_size = len(img_data)

#             # 筛选图片：你可以调低这个大小过滤条件，或去掉这个判断
#             if img_size > 50000:  # 图片大于 50KB
#                 image_path = f"{issue_folder}/image_{count + 1}.jpg"
#                 with open(image_path, 'wb') as f:
#                     f.write(img_data)
#                 print(f"Downloaded {image_path}")
#                 count += 1
#         except Exception as e:
#             print(f"Failed to download image from {img_url}: {e}")


# COMMAND ----------


