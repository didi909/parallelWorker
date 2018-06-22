#!/usr/bin/python
# -*- coding: UTF-8 -*-

import threading
import time
import dbtools
import random
from datetime import datetime

#mode=1 单线程递归  mode=0并发
mode=0
#并发线程数
parallelLevel=10

#id列表
gcIdList=['0531014',
'0280080',
'0101060',
'0531007',
'0531015',
'0351008',
'0101020',
'0271012',
'0532002',
'0371006',
'0532003',
'0531001',
'025001',
'024001',
'0280009',
'0571006',
'0101003',
'0101001',
'05310012',
'0500030',
'0221001',
'0532004',
'0351007',
'028007',
'0100099',
'025003',
'027001',
'05740001',
'0101170',
'0101025']

class myThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self,  name, counter):
        threading.Thread.__init__(self)
        self.name = name
        self.counter = counter
        self.timeElasped = None

    # def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
    #     #print('Starting : "%s"  "%s"'% (self.name,time.ctime(time.time())))
    #     start=datetime.now()
    #     #print_time(self.name, self.counter, 5)
    #     gcId=random.sample(gcIdList,1)[0]
    #     select(gcId)
    #     end=datetime.now()
    #     timeElasped=(end-start).seconds
    #     print('线程： "%s" gcid："%s" 耗费时间: %d秒' %(self.name,gcId,timeElasped))
    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        #print('Starting : "%s"  "%s"'% (self.name,time.ctime(time.time())))
        start=datetime.now()
        #print_time(self.name, self.counter, 5)
        gcId=gcIdList[self.counter]
        select(gcId)
        end=datetime.now()
        timeElasped=(end-start).seconds
        print('线程： "%s" gcid："%s" 耗费时间: %d秒' %(self.name,gcId,timeElasped))

    # def join(self):
    #     threading.Thread.join(self)
    #     return self.timeElasped

def select(gcId):
    db=dbtools.DB()
    sql='''  SELECT
  o.id_, o.table_operate_id_, o.confirm_status_, o.confirm_time_, o.ct_, o.desc_, o.et_, o.is_delete_, o.name_,
  o.audit_user_id_, o.ct_id_, o.money_, o.type_id_, o.big_type_, o.type_, o.indent_type_, o.create_department_,
  o.affirm_status_, o.affirm_time_, o.begin_time_, o.end_time_, o.payee_name_, o.payee_phone_, o.payee_account_,
  o.payee_account_type_, o.address_, o.contract_number_, o.house_id_, o.print_status_, o.print_man_,
  o.print_time_, o.affirm_user_id_, o.practical_money_, o.indent_id_, o.confirm_desc_, o.predict_time_,
  o.repair_id_, o.contract_tn_, o.delete_why_, o.payee_desc_, o.payment_name_, o.payment_phone_,
  o.payment_account_, o.payment_desc_, o.payment_account_type_, o.indent_chengzu_id_, o.create_indent_type_,
  o.parenthouse_id_, o.note_, o.mian_money_, o.delete_date_, o.delete_thoroughly_why_, o.delete_thoroughly_date_,
  o.g_c_id_, o.goto_time_, o.tbs_id_, o.payment_open_bank, o.guest_pools_id_, o.house_pools_id_,
  o.payment_bank_card, o.payee_bank_card, o.payee_open_bank, o.is_todo_, o.table_bank_no_, o.affirm_department_,
  o.audit_department_, o.discount_id_, o.jian_money_, o.adopt_user_id_, o.adopt_user_name_,
  o.adopt_department_id_, o.adopt_department_name_, o.adopt_desc_, o.fitment_id_,
  o.et_id_,o.biz_payment_type_,o.is_correlation_,o.used_deposit_,o.bill_num_,o.affirm_desc_,
  o.payment_method_,o.payer_name_,o.payer_phone_,o.payment_account_no_,o.payment_remark_,
  o.bank_name_,o.branch_bank_name_, o.final_approval_user_id_, o.final_approval_time_, o.final_approval_desc_,o.corporate_account_id_
  FROM table_balance_sheet_ o
  WHERE
  1=1
  and o.is_delete_ = 1
  and o.g_c_id_ = "%s"
''' % (gcId)
    result = db.executeQueryAll(sql)
    #print(result)
    #print(gcId[0])



if __name__ == '__main__':
    totalTime = 0
    if mode == 1:
        count = 0
        for x in gcIdList:
            start = datetime.now()
            select(x)
            end = datetime.now()
            totalTime=totalTime+(end-start).seconds
            print('gcid："%s" 耗费时间: %d秒' % (x, (end - start).seconds))
            count = count +1
        print('总耗时： %d秒' % totalTime)
        print('平均耗时: %d秒' % round(totalTime/count,2))

    elif mode == 0:
        maxTime = 0
        minTime = 1000
        for x in range(0,parallelLevel):
            # 创建新线程
            name="Thread"+str(x)
            thread1 = myThread( name, x)

            # 开启线程
            thread1.start()

print("Exiting Main Thread")

#我需要测试两种场景
#1. 顺序递归 分区/不分区
#2. 并发访问 分区/不分区