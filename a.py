#!/usr/bin/python
# -*- coding: utf-8 -*-
# This Python file uses the following encoding: utf-8
import os
import re
import mmap
import traceback

from multiprocessing import Pool
import time
import argparse


# 根据两个数字和序列的长度找出需要提取的DNA序列位置
def span(_first_num, _second_num, _size):
    first_num_ = int(_first_num)
    second_num_ = int(_second_num)
    size_ = int(_size)
    if first_num_ < second_num_:
        return {'first': (1, first_num_ - 1), 'second': (second_num_ + 1, size_)}
    else:
        return {'first': (1, second_num_ - 1), 'second': (first_num_ + 1, size_)}


# 根据一个范围对象来提取DNA
def extract_dna(data_, span_obj):
    output = ''
    # 先提取第一个序列段
    t = span_obj.get('first')
    str_first = ''
    str_second = ''
    # 如果第一个数字大于第二个数字，说明此DNA序列已经到了端点，可以忽略
    if t[0] < t[1]:
        # result_.write("from: " + str(t[0]) + "  to: " + str(t[1]) + "\n")
        # result_.write(data_[t[0]:t[1]] + "\n")
        # output.join(['from', str(t[0]), ' to: ', str(t[1]), '\n', data_[t[0]:t[1]], '\n'])
        str_first = "from: {0}  to: {1} \n{2}\n".format(str(t[0]), str(t[1]), data_[t[0]:t[1]])

    t = span_obj.get("second")
    if t[0] < t[1]:
        # result_.write("from: " + str(t[0]) + "  to: " + str(t[1]) + "\n")
        # result_.write(data_[t[0]:t[1]] + "\n")
        str_second = "from: {0}  to: {1} \n{2}\n".format(str(t[0]), str(t[1]), data_[t[0]:t[1]])

    return str_first + str_second

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', help='The file to extract query information')
parser.add_argument('-s', '--search', help='The database to be searched in')
args = parser.parse_args()
file_r = args.input
file_to_search = args.search
#
# file_r = 'promoter1.txt'
# file_to_search = 'file2.txt'
# file_r = 'jaz.txt'
# file_to_search = 'All-Unigene.fa'
# index = file_r.read()


# 创建数据输出结果文件
result_file = "result.txt"

comp = re.compile
search = re.search

with open(file_to_search, "r") as f:
    data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

pattern = r'Query=\s*?(?P<query_name>\S+)\s+(?P<query_content>.*?)Lambda'
pattern = comp(pattern, re.S)

pattern2 = r'>lcl\|(?P<name>\w*).*?Length=(?P<size>\d+).*?(?P<query_sbjct>Query [^>]*Sbjct\s*\d*\D*\d*)'
pattern2 = comp(pattern2, re.S)

pattern3 = r'.*?Sbjct\s*(?P<first_num>.*?) .*?(?P<second_num>\d+$)'
pattern3 = comp(pattern3, re.S)


def work(query_content, query_name):
    # print query_name
    # print query_content
    # print 'In work.'
    # 第二步：对于每个Query的内容再进行匹配，得到多个lcl|之后的内容
    output_list = []
    for n in pattern2.finditer(query_content):
        # e3 = time.time()

        # print 'name:  ', n.group('name')
        # print 'size:  ', n.group('size')
        # print n.groups()
        match_dna = None
        try:
            name = n.group('name')
            size = n.group('size')
            matchobj = pattern3.search(n.group('query_sbjct'))
            first_num = matchobj.group('first_num')
            second_num = matchobj.group('second_num')
            posSpan = span(first_num, second_num, size)
            # 对于每条数据，从库中提取需要的DNA序列
            pattern_data = r'>' + name + r'\s*(?P<dna>[^>]+)'
            # print pattern_data
            match_dna = search(pattern_data, data, re.S)

            # list_str = []
            # list_str.append('query_name: ')
            # list_str.append(query_name)
            # list_str.append('\nname: ')
            # list_str.append(name)
            # list_str.append('\nsize: ')
            # list_str.append(size)
            # list_str.append('\nDNA begin pos and end pos:')
            # if int(first_num) > int(second_num):
            #     list_str.append('-')
            # list_str.append('\nfirst_num:  ')
            # list_str.append(first_num)
            # list_str.append('\nsecond_num:  ')
            # list_str.append(second_num)
            # list_str.append('\nThe span needed to extract:')
            # list_str.append(str(posSpan))
            # list_str.append('\n')
            if int(first_num) > int(second_num):
                str_a_sub_query_first = '\nquery_name: {0}\nname: {1}\nsize: {2}\nDNA begin pos and end pos: -\nfirst_num: {3}\nsecond_num: {4}\nThe span needed to extract:{5}\n'.format(
                    query_name, name, size, first_num, second_num, str(posSpan))
            else:
                str_a_sub_query_first = '\nquery_name: {0}\nname: {1}\nsize: {2}\nDNA begin pos and end pos: \nfirst_num: {3}\nsecond_num: {4}\nThe span needed to extract:{5}\n'.format(
                    query_name, name, size, first_num, second_num, str(posSpan))
            if match_dna:
                # print 'Not None'
                dna = match_dna.group('dna')
                # if dna:
                #     print 'dna Not None'
                result_output = ''.join(
                    [str_a_sub_query_first, "DNA series you get: \n", extract_dna("".join(dna.split()), posSpan)])
                # list_str.append("DNA series you get: \n")
                # list_str.append(extract_dna("".join(dna.split()), posSpan))
            else:
                result_output = str_a_sub_query_first

            # result_output = ''.join(list_str)
            output_list.append(result_output)

            # result_.write('query_name: ')
            # result_.write(query_name)
            # result_.write('\n\n')
            # result_.write('name: ')
            # result_.write(name)
            # result_.write('\nsize: ')
            # result_.write(size + '\n')
            # result_.write('DNA begin pos and end pos:')
            # if int(first_num) > int(second_num):
            #     result_.write('  -')
            # result_.write('\nfirst_num:  ')
            # result_.write(first_num)
            # result_.write('\nsecond_num:  ')
            # result_.write(second_num)
            # result_.write('\nThe span needed to extract:')
            # result_.write(str(posSpan) + "\n")
            # # result.write(posSpan.get('first'))
            # # result.write(posSpan.get('second'))
            # # result.write('\n')
            # if match_dna:
            #     dna = match_dna.group('dna')
            #     result_.write("DNA series you get: \n")
            #     result_.write(extract_dna("".join(dna.split()), posSpan, result_))
            # print 1/0
            # print
        except:
            print 'Error!'
            print traceback.print_exc()
        finally:
            pass
        # e4 = time.time()
        # print float(e4-e3)
    return ''.join(output_list)


def write_callback(content):
    with open('result.txt', 'a+') as f__:
        f__.write(content)


if __name__ == '__main__':

    e1 = time.time()

    with open(file_r, "r") as f:
        index = f.read()

    for file in os.listdir(os.getcwd()):
        if os.path.basename(file).startswith('result'):
            os.remove(file)

    pool = Pool()

    # 第一步：先把文本内容按照Query截断
    for m in pattern.finditer(index):
        # print 'query_name:  ', m.group('query_name')
        try:
            pool.apply_async(work, args=(m.group('query_content'), m.group('query_name')), callback=write_callback)
        except:
            traceback.print_exc()
            # print

    pool.close()
    pool.join()
    data.close()
    e2 = time.time()
    print 'Total time elapsed: ', str((float(e2 - e1)))
