from typing import List
import os
import re
import time

import numpy as np
import pandas as pd
from pandas import DataFrame
import requests

import os

from util.widgets import generate_ym, generate_ymd, generate_i

ym = generate_ym()
g = 'cargohose_herren'
bd = f'{g}/{ym}'
bd_a = f'{bd}/a'
b_d = f'{g}_{ym}'

os.makedirs(bd, exist_ok=True)
os.makedirs(bd_a, exist_ok=True)

ymd = generate_ymd()
# i = '(08-09)'
bd_a_img = f'{bd_a}/{ymd}'
bd_a_log = f'{bd_a}/log'
os.makedirs(bd_a_img, exist_ok=True)
os.makedirs(bd_a_log, exist_ok=True)

erp_df: DataFrame = pd.read_excel(f'{bd_a}/product_20240729110524847_verified.xls')

# 如果没有图片链接,说明已下架或者还没更新
erp_df.dropna(subset=['图片链接 1'], inplace=True)

erp_df['父SKU'] = erp_df['父SKU'].fillna(erp_df['SKU'])
erp_df['parent_child'] = np.where(erp_df['父SKU'] == erp_df['SKU'], 'Parent', 'Child')

archived_skus = open('./util/skus/archived_skus.txt', mode='r', encoding='utf-8').readlines()
archived_skus = [i.strip() for i in archived_skus]
other_skus = open('./util/skus/other_skus.txt', mode='r', encoding='utf-8').readlines()
other_skus = [i.strip() for i in other_skus]

erp_df = erp_df[~erp_df['父SKU'].isin(archived_skus + other_skus)]

s1 = [f'图片链接 {i}' for i in range(1, 10)]
s2 = [f'S3图片链接 {i}' for i in range(1, 10)]

src_df = pd.concat([erp_df['父SKU'], erp_df[s1], erp_df[s2]], axis=1)

src_unique_df = pd.DataFrame()

for i in range(1, 1 + 9):
    src_unique_df = pd.concat([src_unique_df, pd.DataFrame(src_df[f'图片链接 {i}'].unique())])

src_unique_df.dropna(subset=[0], inplace=True)
src_unique_df.drop_duplicates(subset=[0], inplace=True)
src_unique_df['fn'] = src_unique_df[0].apply(lambda i: i.rsplit('/', 1)[-1])

import logging
import aiohttp
from aiohttp.client_exceptions import ClientConnectorError, ClientPayloadError
import asyncio
from PIL import Image

# 设置日志
# logging.basicConfig(filename=f'{bd_a_img_log}/log.txt', level=logging.ERROR,
#                     format='%(asctime)s - %(levelname)s - %(message)s')
fp_log = f'{bd_a_log}/log_{ymd}.txt'
logging.basicConfig(filename=fp_log, level=logging.ERROR,
                    format='%(message)s')

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    # 'Host': '183.62.143.166:28099',
    # 'Pragma': 'no-cache',
    # 'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}


async def download_src(u: str):
    fn = u.split('/')[-1]
    fp = f'{bd_a_img}/{fn}'

    if not os.path.exists(fp) or (os.path.exists(fp) and is_image_broken(fp)):
        # 第一次请求获取图片
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(u, headers=headers) as response:
                    if response.status == 200:
                        # downloaded_size = 0

                        with open(fp, 'wb') as f:
                            while True:
                                chunk = await response.content.read(1024)
                                if not chunk:
                                    break
                                f.write(chunk)
                                # downloaded_size += len(chunk)

                        print(f'Success: {u}')
                    else:
                        # 记录异常到日志
                        logging.error(u)
                        print(f'Failed: {u}')
            except ClientConnectorError:
                # 记录异常到日志
                logging.error(u)
                print(f'Failed: {u}')
            except ClientPayloadError:
                # 记录异常到日志
                logging.error(u)
                print(f'Failed: {u}')
    else:
        print(f'图片已存在：{fp}')

async def download_src(u: str):
    fn = u.split('/')[-1]
    fp = f'{bd_a_img}/{fn}'

    # 第一次请求获取图片
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(u, headers=headers) as response:
                if response.status == 200:
                    # downloaded_size = 0

                    with open(fp, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            f.write(chunk)
                            # downloaded_size += len(chunk)

                    print(f'Success: {u}')
                else:
                    # 记录异常到日志
                    logging.error(u)
                    print(f'Failed: {u}')
        except ClientConnectorError:
            # 记录异常到日志
            logging.error(u)
            print(f'Failed: {u}')
        except ClientPayloadError:
            # 记录异常到日志
            logging.error(u)
            print(f'Failed: {u}')


def is_image_broken(image_path):
    try:
        # 打开图片文件
        with Image.open(image_path) as img:
            # 验证图片是否完整
            img.verify()  # 验证图片是否损坏
        return False  # 图片未损坏
    except (IOError, SyntaxError) as e:
        # IOError: 图像文件无法读取
        # SyntaxError: 图像文件格式错误
        return True  # 图片损坏


# 定义每次请求之间的最小延迟
MIN_REQUEST_DELAY = 0


async def main_01():
    tasks = []
    urls = src_unique_df[0].values
    for url in urls:
        # 添加任务到列表
        task = asyncio.create_task(download_src(url))
        tasks.append(task)
        # 异步等待，以确保每次请求之间有足够的延迟
        await asyncio.sleep(MIN_REQUEST_DELAY)

    # 等待所有任务完成
    await asyncio.gather(*tasks)


async def main_02():
    tasks = []
    urls = [i.strip() for i in open(fp_log, 'r').readlines()]
    for url in urls:
        url = re.sub(r'http://.*?/', 'http://183.62.143.166:28099/', url)
        # 添加任务到列表
        task = asyncio.create_task(download_src(url))
        tasks.append(task)
        # 异步等待，以确保每次请求之间有足够的延迟
        await asyncio.sleep(MIN_REQUEST_DELAY)

    # 等待所有任务完成
    await asyncio.gather(*tasks)


# 运行异步任务
asyncio.run(main_01())
asyncio.run(main_02())

print(f"共需下载{len(src_unique_df['fn'].unique())}张图片")
