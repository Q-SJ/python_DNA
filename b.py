#!/usr/bin/python
# -*- coding: utf-8 -*-
# This Python file uses the following encoding: utf-8

import time
from multiprocessing import Pool
import os


def run(fn):
    # print u'参数是：'
    # print fn
    # # fn: 函数参数是数据列表的一个元素
    # time.sleep(1)
    # print u'结果是：'
    # print fn[0] * fn[0]
    list = fn.split(",")
    result = int(list[0]) * 3
    # print "\narg: "
    # print list[0]
    # print "\nresult: "
    # print result
    with open(list[1], 'a+') as f:
        f.write("\narg: " + list[0])
        time.sleep(1)
        f.write("\nresult: " + str(result))
    return result


if __name__ == '__main__':

    for file in os.listdir(os.getcwd()):
        if os.path.basename(file).startswith('result'):
            os.remove(file)

    # testFL = [1, 2, 3, 4, 5, 6]
    testFL = ['1,result5.txt', '2,result5.txt', '3,result5.txt', '4,result5.txt', '5,result5.txt', '6,result5.txt']
    print 'concurrent:'  # 创建多个进程，并行执行
    e1 = time.time()
    pool = Pool(4)  # 创建拥有4个进程数量的进程池  # testFL:要处理的数据列表，run：处理testFL列表中数据的函数
    rl = pool.map_async(run, testFL)
    pool.close()  # 关闭进程池，不再接受新的进程
    pool.join()  # 主进程阻塞等待子进程的退出
    e2 = time.time()
    print "并行执行时间：", float(e2 - e1)
    print rl
