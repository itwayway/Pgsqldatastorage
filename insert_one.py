#!/usr/bin/env python3
# -*- coding: utf-8 -*- 

import psycopg2
import time
import csv
import traceback





def get_conn():
    conn = psycopg2.connect(host="*******",
                            user="*******",
                            password="*******",
                            database="*******")
    cur = conn.cursor()
    return cur, conn


def close_conn(cur, conn):
    if cur:
        cur.close()
    if conn:
        conn.close()




data_list = []
path = './测试表.csv' #  默认目录
try:

    time_start=time.time()

    with open(path, "r", newline='') as f:
        data = csv.reader(f)
        # print(type(data))
        # for rowin in data:
        #     print(rowin)
        #     data_list.append(rowin)
        # print(data_list)

        # print(data)
        header = f.readlines()
        print(len(header))
        for i in range(1,len(header)):
            head = header[i].strip()
            # print(head)
            head_list = head.split(',')
        # print(head_list)
 
       
            data_list.append(head_list)


        cur, conn = get_conn()
        sql_crea = '''CREATE TABLE if not exists ds(
        cell_id        int8,
        tel_traffic    float8,
        traffic        float8,
        user_num       int8,
        date           timestamp(6))'''

        
        sql = "insert into ds(cell_id, tel_traffic, traffic, user_num, date) values (%s, %s, %s, %s, %s)"
        sql_add1 = '''ALTER TABLE ds ADD total float8'''
        sql_add2 = '''ALTER TABLE ds ADD user_total float8'''
        sql_select = '''select * from ds'''
        sql_tru = '''truncate table ds'''
        sql_del = '''drop table ds'''
        sql_sum = '''update ds set total = tel_traffic + traffic'''
        sql_div = '''update ds set user_total = Case When user_num = 0 Then null Else total / user_num End'''
        update_time = "update ds  set date = date + interval '3 hours'"
        sql_out = "copy (select * from ds)  to '/var/lib/pgsql/SQL测试题1结果.csv' with csv header"
                    # select tel_traffic + traffic as total
                    # select sum(tel_traffic+traffic) from ds group by tel_traffic,traffic
                    # select tel_traffic, traffic, tel_traffic + traffic total from ds
        cur.execute(sql_crea)
        cur.execute(sql_select)
        
        if cur.fetchall():
            cur.execute(sql_del)        #如果有数据就删除表
            cur.execute(sql_crea)       #建表

        for i in data_list:

            # print(i)
            cur.execute(sql, i)
            # conn.commit()

        cur.execute(sql_add1)         #新增字段total
        cur.execute(sql_add2)         #新增字段user_total
        cur.execute(sql_sum)          #更新total字段
        cur.execute(sql_div)          #更新user_total字段
        cur.execute(update_time)      #更新date字段时间
        cur.execute(sql_out)
        # print(cur.fetchone()[0])
        # print(cur.fetchone())
        conn.commit()
        
        close_conn(cur, conn)
        f.close()
    
    time_end=time.time()
    print("执行成功:",1)
    print("输入文件名:", path)
    print("输出文件名:", "/var/lib/pgsql/SQL测试题1结果.csv")           #服务器地址
    print('''新增总业务量及个人业务量字段(total,user_total),并赋值
            调整date字段时间,顺延3小时''')
    print('cost time:', time_end-time_start)
except:
    traceback.print_exc()
    print("执行失败:",0)

    