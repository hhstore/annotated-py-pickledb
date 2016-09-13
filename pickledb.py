#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015, Harrison Erd
# All rights reserved.

import os
import simplejson


############################################################
#                      接口
############################################################

def load(location, option):
    '''Return a pickledb object. location is the path to the json file.'''
    return pickledb(location, option)  # 返回 pickledb 类对象


############################################################
#                      核心类
# 说明:
#   - 1. 代码非常简单, 都是 Python 的 list, dict 的基本操作
#   - 2. 利用 json 模块, 读写磁盘文件(当作数据库文件)
#   - 3. 接口 API, 完全同 redis.
#   - 4. 代码没有难以理解的. 通过此实现, 可以简单山寨一个 redis.
############################################################

class pickledb(object):
    def __init__(self, location, option):
        '''Creates a database object and loads the data from the location path.
        If the file does not exist it will be created on the first update.'''
        self.load(location, option)     # 载入磁盘数据, or 初始化为空

    def load(self, location, option):
        ''' 从磁盘文件读入数据到 内存字典, or 将内存字典, 初始化为空
        Loads, reloads or changes the path to the db file.'''
        location = os.path.expanduser(location)
        self.loco = location   # 数据库文件存储路径
        self.fsave = option    # 此参数=[true, false], 决定是否立刻将所有内存操作, 更新到磁盘文件

        # 若存在数据库文件, 读入数据
        # 若不存在, 初始化为空
        if os.path.exists(location):
            self._loaddb()    # 载入磁盘数据
        else:
            self.db = {}      # 初始化内存 db 字典为空
        return True

    def dump(self):
        ''' 将内存中的数据, 强制写出到磁盘文件
        Force dump memory db to file.'''
        self._dumpdb(True)
        return True

    def set(self, key, value):
        '''Set the (string,int,whatever) value of a key'''
        self.db[key] = value      # 内存字典 记录值
        self._dumpdb(self.fsave)  # 根据选项, 决定要不要 立刻写出到磁盘.
        return True

    def get(self, key):
        '''Get the value of a key'''
        try:
            return self.db[key]  # 内存 db 读出值
        except KeyError:
            return None

    def getall(self):
        ''' 取出所有 key
        Return a list of all keys in db'''
        return self.db.keys()

    def rem(self, key):
        ''' 删除一个 key
        Delete a key'''
        del self.db[key]
        self._dumpdb(self.fsave)  # 决定要不要立即更新磁盘文件.
        return True

    ############################################################
    #                list 类型数据操作
    ############################################################

    def lcreate(self, name):
        ''' 创建一个列表
        Create a list'''
        self.db[name] = []
        self._dumpdb(self.fsave)  # 是否立即保存到磁盘
        return True

    def ladd(self, name, value):
        ''' 添加元素到指定列表
        Add a value to a list'''
        self.db[name].append(value)  # 添加
        self._dumpdb(self.fsave)     # 是否立即保存到磁盘
        return True

    def lextend(self, name, seq):
        ''' 将所给 list, 扩展到 目标 list 中
        Extend a list with a sequence'''
        self.db[name].extend(seq)  # 列表扩展
        self._dumpdb(self.fsave)   # 是否立即保存到磁盘
        return True

    def lgetall(self, name):
        ''' 取出 list 全部内容
        Return all values in a list'''
        return self.db[name]

    def lget(self, name, pos):
        ''' 取出给定下标位置的 list 元素
        Return one value in a list'''
        return self.db[name][pos]

    def lrem(self, name):
        ''' 删除指定 list, 并返回其长度
        Remove a list and all of its values'''
        number = len(self.db[name])  # 待删除 list 的长度
        del self.db[name]
        self._dumpdb(self.fsave)     # 是否立即更新到磁盘
        return number

    def lpop(self, name, pos):
        ''' 取出给定下标位置的 list 元素, 并将该元素从 list 中移除
        Remove one value in a list'''
        value = self.db[name][pos]  # 取出 list 元素
        del self.db[name][pos]      # 将该元素从 list 中移除
        self._dumpdb(self.fsave)    # 是否立即更新到磁盘
        return value

    def llen(self, name):
        ''' 取出 list 长度
        Returns the length of the list'''
        return len(self.db[name])

    def append(self, key, more):
        ''' 拼接 字符串元素: 给定 key 的元素, 作字符串拼接
        Add more to a key's value'''
        tmp = self.db[key]
        self.db[key] = ('%s%s' % (tmp, more))  # 字符串拼接
        self._dumpdb(self.fsave)
        return True

    def lappend(self, name, pos, more):
        ''' 拼接 list 中元素: 给的 key,pos 的元素, 作字符串拼接
        Add more to a value in a list'''
        tmp = self.db[name][pos]
        self.db[name][pos] = ('%s%s' % (tmp, more))  # 字符串
        self._dumpdb(self.fsave)
        return True

    ############################################################
    #                dict 类型数据操作
    ############################################################

    def dcreate(self, name):
        ''' 创建 dict 类型对象
        Create a dict'''
        self.db[name] = {}         # 字典类型数据
        self._dumpdb(self.fsave)   # 是否立即更新到磁盘
        return True

    def dadd(self, name, pair):
        ''' 把 元组对参数, 存入指定 dict 对象
        Add a key-value pair to a dict, "pair" is a tuple'''
        self.db[name][pair[0]] = pair[1]    # 存入 元组对(v1, v2) 数据
        self._dumpdb(self.fsave)            # 是否立即更新到磁盘
        return True

    def dget(self, name, key):
        ''' 取出 dict 指定 key 元素数据
        Return the value for a key in a dict'''
        return self.db[name][key]

    def dgetall(self, name):
        ''' 取出 dict 全部内容
        Return all key-value pairs from a dict'''
        return self.db[name]

    def drem(self, name):
        ''' 清空目标 dict 全部数据
        Remove a dict and all of its pairs'''
        del self.db[name]
        self._dumpdb(self.fsave)    # 是否立即更新到磁盘
        return True

    def dpop(self, name, key):
        ''' 取出目标 dict 指定 key 元素, 并将该元素从 dict 中移除
        Remove one key-value pair in a dict'''
        value = self.db[name][key]
        del self.db[name][key]
        self._dumpdb(self.fsave)
        return value

    def dkeys(self, name):
        ''' 取出目标 dict 全部 key
        Return all the keys for a dict'''
        return self.db[name].keys()

    def dvals(self, name):
        ''' 取出目标 dict 全部 value 值
        Return all the values for a dict'''
        return self.db[name].values()

    def dexists(self, name, key):
        ''' 判断目标 dict 中是否存在所给 key
        Determine if a key exists or not'''
        if self.db[name][key] is not None:
            return 1
        else:
            return 0

    ############################################################
    #                        底层接口
    ############################################################

    def deldb(self):
        ''' 清空数据库
        Delete everything from the database'''
        self.db = {}
        self._dumpdb(self.fsave)
        return True

    def _loaddb(self):
        ''' 读入数据库数据
        Load or reload the json info from the file'''
        # 1. 打开数据库文件.
        # 2. 并使用 json 解析
        self.db = simplejson.load(open(self.loco, 'rb'))

    def _dumpdb(self, forced):
        ''' 数据写出到数据库
        Write/save the json dump into the file'''
        if forced:
            # 把 json 数据, 写出到数据库文件.
            simplejson.dump(self.db, open(self.loco, 'wt'))
