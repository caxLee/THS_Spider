import re
from playwright.sync_api import sync_playwright
from seatable_api import Base
from datetime import datetime
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()
API_TOKEN = os.getenv('SEATABLE_API_TOKEN')
SERVER_URL = os.getenv('SEATABLE_SERVER_URL')
TABLE_NAME = "龙虎榜"

def is_chinese(text):
    return bool(re.search(r'[\u4e00-\u9fff]', text))

def fetch_longhu_data():
    data = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        url_main = "https://data.10jqka.com.cn/market/longhu/"
        page.goto(url_main)
        page.wait_for_selector("table.m-table", timeout=60000)
        rows_main = page.query_selector_all("table.m-table tbody tr")
        for row in rows_main:
            cells = row.query_selector_all("td")
            row_data = [cell.inner_text() for cell in cells]
            data.append(row_data)
            
        url_rank = "https://data.10jqka.com.cn/ifmarket/lhbyyb/type/1/tab/sbcs/field/sbcs/sort/desc/page/3/"
        page.goto(url_rank)
        page.wait_for_selector("table.m-table", timeout=60000)
        rows_rank = page.query_selector_all("table.m-table tbody tr")
        for row in rows_rank:
            cells = row.query_selector_all("td")
            row_data = [cell.inner_text() for cell in cells]
            data.append(row_data)
            
        browser.close()
    return data

def split_data_extended(data):
    front, back, third = [], [], []
    for row in data:
        if len(row) > 1 and is_chinese(row[1]):
            third.append(row)
        elif len(row) > 5 and row[5].strip() != "":
            front.append(row)
        else:
            back.append(row)
    return front, back, third

def upload_to_seatable(data, table_name=TABLE_NAME):
    try:
        base = Base(API_TOKEN, SERVER_URL)
        base.auth()

        # 分离数据并只使用front数据
        front, _, _ = split_data_extended(data)
        
        # 获取当前日期
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # 准备数据
        columns = ["时效", "代码", "名称", "现价", "涨跌幅", "成交金额", "净买入额", "更新时间"]
        rows = []
        for row in front:
            row_dict = dict(zip(columns[:-1], row))  # 除了更新时间的所有列
            row_dict["更新时间"] = current_date
            rows.append(row_dict)

        try:
            metadata = base.get_metadata()
            table_exists = any(table['name'] == table_name for table in metadata['tables'])
            
            if not table_exists:
                print(f"表 {table_name} 不存在，开始创建...")
                # 创建表和列
                columns_def = [{"name": col, "type": "text"} for col in columns]
                base.add_table(table_name, columns_def)
                print(f"表 {table_name} 创建成功")

            # 清空现有数据
            try:
                existing_rows = base.list_rows(table_name)
                if existing_rows:
                    base.delete_rows(table_name, [row['_id'] for row in existing_rows])
                    print("已清空现有数据")
            except Exception as e:
                print(f"清空数据失败: {e}")

            # 批量上传数据
            batch_size = 50
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                base.batch_append_rows(table_name, batch)
                print(f"已上传: {min(i + batch_size, len(rows))}/{len(rows)} 条记录")

            print(f"数据更新完成，共更新 {len(rows)} 条记录")
            return True

        except Exception as e:
            print(f"表操作失败: {e}")
            return False

    except Exception as e:
        print(f"连接失败: {e}")
        return False

if __name__ == "__main__":
    print("龙虎榜数据抓取开始运行，有头模式，请勿关闭弹出的浏览器")
    data = fetch_longhu_data()
    if data:
        if upload_to_seatable(data):
            print("数据已成功上传到 SeaTable")
        else:
            print("数据上传到 SeaTable 失败")
    else:
        print("未能获取数据")