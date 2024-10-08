﻿import pymysql
import pandas
from datetime import datetime

# 设置MySQL连接信息
MYSQL_USER = 'report'
MYSQL_PASSWORD = 'Ev9@EPQ_Zt'
MYSQL_HOST = '133.0.132.51'
MYSQL_PORT = 54588
MYSQL_DB = 'report'

# 获取当前日期
current_date = datetime.now()
today = current_date.date()


# 连接到 MySQL 数据库
dbconn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
# 建立游标
cursor = dbconn.cursor()
# 创建Pandas空Dataframe， 表1

Table_3 = pandas.DataFrame()

# 创建分公司列表
regions = ['孝南', '云梦', '大悟', '新城', '孝昌', '安陆', '汉川', '应城']
# 设置表格列
Table_3_new_col = ["感知-派单数量","感知-已修复", "感知-未修复", "感知-在途", "感知-感知修复", "质差-派单数量", "质差-已修复", "质差-未修复", "质差-在途", "质差-感知修复", "总整治成功率-总质差率", "总整治成功率-全市在途"]
# 测试区域

# 表修改操作，删除 质差工单 未修复且网关承载不足派单
def delete_tablezc():
    sql = "SELECT * FROM tablezc WHERE col3 = '未修复' AND col24 = '网关承载不足派单'"
    delete_num = cursor.execute(sql)
    if delete_num != 0:
        print("有残余未删除工单数：{}".format(delete_num))
        sql = "DELETE FROM tablezc WHERE col3 = '未修复' AND col24 = '网关承载不足派单'"
        actually_delete_num = cursor.execute(sql)
        dbconn.commit()
        if actually_delete_num != delete_num:
            print("实际删除数与搜寻删除数有差异，注意错误产生原因。")
        else:
            print("实际删除数与搜寻删除数无差异，已完成删除冗余数据源操作。")
    else:
        print("今日数据库删除操作已更新，无需删除数据")
 
# 错误检查：分支局变化
def error_check_substation():
    sql = "SELECT DISTINCT col42 FROM tablezc"
    num = cursor.execute(sql)
    with open("./Lib/sub.txt", "r", encoding='utf-8') as stock_sub:
        stock_sub_lines = stock_sub.readlines()
        for line in stock_sub_lines:
            line = line.strip('\n')  #去掉列表中每一个元素的换行符

    with open("./Lib/ignore_sub.txt", "r", encoding='utf-8') as stock_ignnre_sub:
        stock_ignnre_sub_lines = stock_ignnre_sub.readlines()
        for line in stock_ignnre_sub_lines:
            line = line.strip('\n')  #去掉列表中每一个元素的换行符
    
    new_sub = cursor.fetchall()

    if num == stock_ignnre_sub_lines.__len__() + stock_sub_lines.__len__():
        print("分支局结构稳定无变化")
    elif num < stock_ignnre_sub_lines.__len__() + stock_sub_lines.__len__():
        print("分支局总数减少，请检查output_sub内分支局名单，如有异常请手动更新")
        with open("./Lib/output_sub.txt", 'w') as file:
            for result in new_sub:
                file.write(result[0] + "\n")
    else:
        print("分支局数量增加,请检查increase_sub内分支局名单，如有异常请手动更新sub文件或者ignore_sub文件")
        old_sub = stock_sub_lines + stock_ignnre_sub_lines
        increase_sub = list(set(new_sub) - set(old_sub))
        with open("./Lib/increase_sub.txt", 'w') as file:
            for result in increase_sub:
                file.write(result[0] + "\n")

# 执行查询：分支局筛选
def select_table_region():
    for region in regions:
        sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col = %s"
        num = cursor.execute(sql, (region + "%", today))
        Table_3.loc[region, "感知-派单数量"] = num

        sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col3 = '已修复' AND col = %s"
        num = cursor.execute(sql, (region + "%", today))
        Table_3.loc[region, "感知-已修复"] = num

        sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col40 = 'nan' AND col = %s"
        num = cursor.execute(sql, (region + "%", today))
        Table_3.loc[region, "感知-在途"] = num

        sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col3 = '未修复' AND col = %s"
        num = cursor.execute(sql, (region + "%", today)) - num
        Table_3.loc[region, "感知-未修复"] = num

        sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col = %s"
        num = cursor.execute(sql, (region + "%", today))
        Table_3.loc[region, "质差-派单数量"] = num

        sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col3 = '已修复' AND COL40 != 'nan' AND col = %s"
        num = cursor.execute(sql, (region + "%", today))
        Table_3.loc[region, "质差-已修复"] = num

        sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col40 = 'nan' AND col = %s"
        num = cursor.execute(sql, (region + "%", today))
        Table_3.loc[region, "质差-在途"] = num

        sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col3 = '未修复' AND COL40 != 'nan' AND col = %s"
        num = cursor.execute(sql, (region + "%", today))
        Table_3.loc[region, "质差-未修复"] = num

    Table_3["感知-感知修复"] =Table_3["感知-已修复"] / Table_3["感知-派单数量"]


# 执行查询：分支局筛选
def select_table_substation():
    sql = "SELECT DISTINCT col42 FROM tablezc"
    num = cursor.execute(sql)
    print(num)
    results = cursor.fetchall()
    with open("./Lib/Output.txt", 'w') as file:
        for result in results:
            file.write(result[0] + "\n")
    # for region in regions:
    #     sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today))
    #     Table_3.loc[region, "感知-派单数量"] = num

    #     sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col3 = '已修复' AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today))
    #     Table_3.loc[region, "感知-已修复"] = num

    #     sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col40 = 'nan' AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today))
    #     Table_3.loc[region, "感知-在途"] = num

    #     sql = "SELECT * FROM tablemyd WHERE col42 LIKE %s AND col3 = '未修复' AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today)) - num
    #     Table_3.loc[region, "感知-未修复"] = num

    #     sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today))
    #     Table_3.loc[region, "质差-派单数量"] = num

    #     sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col3 = '已修复' AND COL40 != 'nan' AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today))
    #     Table_3.loc[region, "质差-已修复"] = num

    #     sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col40 = 'nan' AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today))
    #     Table_3.loc[region, "质差-在途"] = num

    #     sql = "SELECT col1 FROM tablezc WHERE col42 LIKE %s AND col3 = '未修复' AND COL40 != 'nan' AND col = %s"
    #     num = cursor.execute(sql, (region + "%", today))
    #     Table_3.loc[region, "质差-未修复"] = num

    # Table_3["感知-感知修复"] =Table_3["感知-已修复"] / Table_3["感知-派单数量"]

# 文本实现：文本6， 表3
def chart_zc(Table_3):
    Table_3["质差-感知修复"] =Table_3["质差-已修复"] / Table_3["质差-派单数量"]
    Table_3["总整治成功率-全市在途"] = Table_3["感知-在途"] + Table_3["质差-在途"]
    Table_3["总整治成功率-总质差率"] = (Table_3["感知-已修复"] + Table_3["质差-已修复"]) / (Table_3["感知-派单数量"] + Table_3["质差-派单数量"]) 
    sorted_Table_3 = Table_3.sort_values(by = "总整治成功率-总质差率", ascending = True)
    Txt = "{one}、{two}和{three}总质差工单整治成功率排全市后三位".format(one = sorted_Table_3.index[0], two = sorted_Table_3.index[1], three = sorted_Table_3.index[2])
    return Txt

def Trans_Table(table, table_new_col):
    for col in table.columns.tolist():
        table.loc["全市", col] = table.loc[:, col].sum()
    table = table[table_new_col]
    return table

def Trans_Table_mini(table):
    for col in table.columns.tolist():
        table.loc["全市", col] = table.loc[:, col].sum()
    return table

# 函数使用区域


# select_tablemyd()
# select_tablezc()
# chart_zc(Table_3)
delete_tablezc()
# select_table_region()
error_check_substation()
# select_table_substation()
# 装维在途工单统计（Table_1）处理区域

# Table_3 = Trans_Table(Table_3, Table_3_new_col)


# Table_3.loc["全市", "感知-感知修复"] =Table_3.loc["全市", "感知-已修复"] / Table_3.loc["全市", "感知-派单数量"]
# Table_3.loc["全市", "质差-感知修复"] =Table_3.loc["全市", "质差-已修复"] / Table_3.loc["全市", "质差-派单数量"]
# Table_3.loc["全市", "总整治成功率-全市在途"] = Table_3.loc["全市", "感知-在途"] + Table_3.loc["全市", "质差-在途"]
# Table_3.loc["全市", "总整治成功率-总质差率"] = (Table_3.loc["全市", "感知-已修复"] + Table_3.loc["全市", "质差-已修复"]) / (Table_3.loc["全市", "感知-派单数量"] + Table_3.loc["全市", "质差-派单数量"]) 

# Table_3 = Table_3.round(2)

cursor.close()
dbconn.close()