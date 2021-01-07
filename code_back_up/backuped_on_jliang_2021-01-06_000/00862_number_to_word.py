# -*- coding: utf-8 -*-
# File Name: etl_pos.py
# Author: Ben Ho
# Editor: Ben Ho
# Created: 2020/06/13
# Last Edited: 2020/06/13
# Description: Convert Number to English Words

def NumberToWords(num):
    """
    :type num: int
    :rtype: str
    """
    def one(num):
        switcher = {
            1: 'one',
            2: 'two',
            3: 'three',
            4: 'four',
            5: 'five',
            6: 'six',
            7: 'seven',
            8: 'eight',
            9: 'nine'
        }
        return switcher.get(num)

    def two_less_20(num):
        switcher = {
            10: 'ten',
            11: 'eleven',
            12: 'twelve',
            13: 'thirteen',
            14: 'fourteen',
            15: 'fifteen',
            16: 'sixteen',
            17: 'seventeen',
            18: 'eighteen',
            19: 'nineteen'
        }
        return switcher.get(num)

    def ten(num):
        switcher = {
            2: 'twenty',
            3: 'thirty',
            4: 'forty',
            5: 'fifty',
            6: 'sixty',
            7: 'seventy',
            8: 'eighty',
            9: 'ninety'
        }
        return switcher.get(num)

    def two(num):
        if not num:
            return ''
        elif num < 10:
            return one(num)
        elif num < 20:
            return two_less_20(num)
        else:
            tenner = num // 10
            rest = num - tenner * 10
            return ten(tenner) + '_' + one(rest) if rest else ten(tenner)

    def three(num):
        hundred = num // 100
        rest = num - hundred * 100
        if hundred and rest:
            return one(hundred) + '_Hundred_' + two(rest)
        elif not hundred and rest:
            return two(rest)
        elif hundred and not rest:
            return one(hundred) + '_Hundred'

    billion = num // 1000000000
    million = (num - billion * 1000000000) // 1000000
    thousand = (num - billion * 1000000000 - million * 1000000) // 1000
    rest = num - billion * 1000000000 - million * 1000000 - thousand * 1000

    if not num:
        return 'Zero'

    result = ''
    if billion:
        result = three(billion) + '_Billion'
    if million:
        result += '_' if result else ''
        result += three(million) + '_Million'
    if thousand:
        result += '_' if result else ''
        result += three(thousand) + '_Thousand'
    if rest:
        result += '_' if result else ''
        result += three(rest)
    return result
