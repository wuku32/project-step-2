import os
import time

import pandas as pd
from pandas import DataFrame
import requests

headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Connection': 'keep-alive',
    'Cookie': 'SESSION=ZGEyZWU3YTEtMmY2MC00ODQ1LWIyYjMtZjljYTBjZmEyNTVh; Admin-Token=da2ee7a1-2f60-4845-b2b3-f9ca0cfa255a',
    'Host': 'gzerpapp.ser.ltd:28085',
    'Origin': 'http://gzerpapp.ser.ltd:28085',
    'Referer': 'http://gzerpapp.ser.ltd:28085/product/search/image',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
    'X-Auth-Token': 'da2ee7a1-2f60-4845-b2b3-f9ca0cfa255a',
}

url = 'http://gzerpapp.ser.ltd:28085/product/api/image/searchs/list'

data = {
    'imageSearchType': '1'
}

import os

from util.widgets import generate_ym, generate_ymd, generate_ymd_HMS, generate_i

ym = generate_ym()
g = 'cargohose_herren'
bd = f'{g}/{ym}'
bd_a = f'{bd}/a'
b_d = f'{g}_{ym}'

os.makedirs(bd, exist_ok=True)
os.makedirs(bd_a, exist_ok=True)

ymd = generate_ymd()
ymd_HMS = generate_ymd_HMS()
i = '(08-09)'
bd_a_img = f'{bd_a}/{ymd}'
os.makedirs(bd_a_img, exist_ok=True)

bd_IMG = f'{bd}/{ymd}_{i}'

os.makedirs(bd, exist_ok=True)
os.makedirs(bd_IMG, exist_ok=True)

fp_tgt = f'{g}/{g}_tgt.xlsx'
ori_df: DataFrame = pd.read_excel(fp_tgt, sheet_name='tgt')

sfs = ['default', 'asc', 'desc', 'cmt', 'newr', 'best']
ts = []

for sf in sfs:
    bd_img = f'{bd_IMG}/{sf}'
    for j, fn in enumerate(os.listdir(bd_img)):
        print(f'***{j + 1:02d}: {sf}/{fn}***')
        fp = f'{bd_img}/{fn}'
        files = {
            'file': (fn, open(fp, 'rb'), 'image/jpeg')
        }
        response = requests.post(url, headers=headers, data=data, files=files)
        # Make sure to close the file after sending the request.
        files['file'][1].close()

        items = response.json()['data']
        for item in items:
            t = {
                'sf': sf,
                'fp': fp,
                'imageUrl': item['imageUrl'],
                'productSku': item['productSku'],
                'amazon_psku': '',
                'ASIN': '',
                'note': '',
                'productLabelList': item['productLabelList'],
                'categoryName': item['categoryName'],
                'warehouseName': item['warehouseName'],
                'developerName': item['developerName'],
                'buyerName': item['buyerName'],
                'vectorDistance': item['vectorDistance'],
                'buyingPrice': item['buyingPrice'],
                'material': item['material'],
                'withholdStock': item['withholdStock'],
                'productSkuHref': item['productSkuHref'],

                # 'JPY_feeValue': item['suggestPriceList'][0]['feeValue'],
                # 'JPY_freight': item['suggestPriceList'][0]['freight'],
                # 'JPY_maxIntFee': item['suggestPriceList'][0]['maxIntFee'],
                # 'JPY_shippingCost': item['suggestPriceList'][0]['shippingCost'],
                #
                # 'EUR_feeValue': item['suggestPriceList'][1]['feeValue'],
                # 'EUR_freight': item['suggestPriceList'][1]['freight'],
                # 'EUR_maxIntFee': item['suggestPriceList'][1]['maxIntFee'],
                # 'EUR_shippingCost': item['suggestPriceList'][1]['shippingCost'],
                #
                # 'USD_feeValue': item['suggestPriceList'][2]['feeValue'],
                # 'USD_freight': item['suggestPriceList'][2]['freight'],
                # 'USD_maxIntFee': item['suggestPriceList'][2]['maxIntFee'],
                # 'USD_shippingCost': item['suggestPriceList'][2]['shippingCost'],
                #
                # 'GBP_feeValue': item['suggestPriceList'][3]['feeValue'],
                # 'GBP_freight': item['suggestPriceList'][3]['freight'],
                # 'GBP_maxIntFee': item['suggestPriceList'][3]['maxIntFee'],
                # 'GBP_shippingCost': item['suggestPriceList'][3]['shippingCost'],
            }
            ts.append(t)

        new_df: DataFrame = pd.DataFrame(ts)
        new_df['similarity'] = 1 - new_df['vectorDistance']

        from util.widgets import Status

        new_df['ymd'] = ymd
        new_df['status'] = Status.UNUSED.value
        new_df['ymd_HMS'] = ''

        df = pd.concat([ori_df, new_df], axis=0, ignore_index=True)

        with pd.ExcelWriter(fp_tgt) as writer:
            df.to_excel(writer, index=False, sheet_name='tgt')
        time.sleep(5)

tgt_df: DataFrame = pd.read_excel(fp_tgt, sheet_name='tgt')
from util.widgets import note_inuse_sku, note_archived_sku, note_deleted_sku, note_recalled_sku, note_other_sku

tgt_df = note_inuse_sku(tgt_df)
tgt_df = note_archived_sku(tgt_df)
tgt_df = note_deleted_sku(tgt_df)
tgt_df = note_recalled_sku(tgt_df)
tgt_df = note_other_sku(tgt_df)

tgt_df.drop_duplicates(subset=['productSku', 'status'], keep='last', inplace=True)
tgt_df.loc[tgt_df.duplicated(subset=['productSku']), 'status'] = Status.IN_USE.value
tgt_df.drop_duplicates(subset=['productSku', 'status'], keep='first', inplace=True)
tgt_df.loc[tgt_df.duplicated(subset=['productSku']), 'status'] = Status.ARCHIVED.value
tgt_df.drop_duplicates(subset=['productSku', 'status'], keep='first', inplace=True)
tgt_df.loc[tgt_df.duplicated(subset=['productSku']), 'status'] = Status.DELETED.value
tgt_df.drop_duplicates(subset=['productSku', 'status'], keep='first', inplace=True)
tgt_df.loc[tgt_df.duplicated(subset=['productSku']), 'status'] = Status.RECALLED.value
tgt_df.drop_duplicates(subset=['productSku', 'status'], keep='first', inplace=True)
tgt_df.loc[tgt_df.duplicated(subset=['productSku']), 'status'] = Status.OTHER.value
tgt_df.drop_duplicates(subset=['productSku', 'status'], keep='first', inplace=True)
with pd.ExcelWriter(fp_tgt) as writer:
    tgt_df.to_excel(writer, index=False, sheet_name='tgt')
