#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

'''
本模块专用于处理sql逻辑，生成sql脚本
'''
import pymysql
import configparser
import os
import commontools
import datetime



#定义日志
logger=commontools.logger

#读取数据库配置
cp = configparser.ConfigParser()
cp.read('main.conf')

host=cp.get('db', 'host')
port=int(cp.get('db', 'port'))
user=cp.get('db', 'user')
passwd=cp.get('db', 'pass')
dbname=cp.get('db', 'dbname')
charset=cp.get('db','charset')

#常量
today=(datetime.datetime.now()).strftime('%Y-%m-%d 00:00:00')

class DB():
    def __init__(self):
        self.dbopen=False

    #建立连接
    def connect(self):
        if self.dbopen == False:
            print('connect db...')
            #self.connection = cx_Oracle.connect(user+'/'+passwd+'@'+host+'/'+dbname)
            #start = datetime.datetime.now()
            self.connection = pymysql.connect(host,user,passwd,dbname,port,charset="utf8")
            #end = datetime.datetime.now()
            #print('数据库连接耗费时间: %d秒' % ((end - start).seconds))
            self.dbopen = True

    #执行DML
    def executeSql(self,sql):
        self.connect()
        self.executor=self.connection.cursor()
        return self.executor.execute(sql)

    def executeQueryOne(self , sql):
        self.executeSql(sql)
        return self.executor.fetchone()

    #返回key=value 字典
    def executeQueryAll(self , sql):
        self.executeSql(sql)
        return self.executor.fetchall()



    # def executeUpdate(self ,sql='' , isAutoCommit=False):
    #     c = self.executeSql(sql)
    #     if isAutoCommit == True:
    #         self.commit() #提交事务
    #         return c

    #提交事务
    def commit(self):
        self.connection.commit() #提交事务


    #关闭数据库，释放资源
    def closeDB(self):
        if not self.connection is None:
            print('close db...')
            #self.connection.commit() #提交事务
            self.connection.close()

    def rollback(self):
        self.connection.rollback()

def insertDB(sqlString):
    db=DB()
    try:
        logger.debug(sqlString)
        db.executeSql(sqlString)
        db.commit()
        db.closeDB()
    except Exception as e:
        logger.error(str(e))
        db.rollback()
        db.closeDB()

def selectMsgToSend():
    db=DB()
    format='%Y-%m-%d %H:%i:%S'
    try:
        #只发送当天未发送的短信
        sqlString = '''select id,date_format(create_time,"%s"),server_name,log_name,phone from send_record where STATUS='未发送' and create_time > "%s" ''' % (format,today)
        logger.debug(sqlString)
        result=db.executeQueryAll(sqlString)
        #db.commit()
        db.closeDB()
        return result
    except Exception as e:
        logger.error(str(e))
        db.rollback()
        db.closeDB()

def updateSendRecord(id,status):
    db=DB()
    format='%Y-%m-%d %H:%i:%S'
    try:
        #根据返回结果更新记录
        sqlString = '''update send_record set STATUS="%s" where id = %d ''' % (status,id)
        logger.debug(sqlString)
        result=db.executeSql(sqlString)
        db.commit()
        db.closeDB()
        return result
    except Exception as e:
        logger.error(str(e))
        db.rollback()
        db.closeDB()
