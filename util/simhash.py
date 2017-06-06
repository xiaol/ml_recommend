# -*- coding: utf-8 -*-
# @Time    : 17/3/15 下午6:40
# @Author  : liulei
# @Brief   : 
# @File    : simhash.py
# @Software: PyCharm Community Edition

class simhash():
    def __init__(self, tokens='', hashbits=64):
        self.hashbits = hashbits
        self.hash = self.simhash(tokens)

    def __str__(self):
        return str(self.hash)

    def __long__(self):
        return long(self.hash)

    def __float__(self):
        return float(self.hash)

    def simhash(self, tokens):
        # Returns a Charikar simhash with appropriate bitlength
        v = [0] * self.hashbits

        for t in [self._string_hash(x) for x in tokens]:
            bitmask = 0
            for i in range(self.hashbits):
                bitmask = 1 << i
                # print(t,bitmask, t & bitmask)
                if t & bitmask:
                    v[i] += 1  # 查看当前bit位是否为1，是的话则将该位+1
                else:
                    v[i] += -1  # 否则得话，该位减1

        fingerprint = 0
        for i in range(self.hashbits):
            if v[i] >= 0:
                fingerprint += 1 << i
                # 整个文档的fingerprint为最终各个位大于等于0的位的和
        return fingerprint

    #计算一个词的hash值。 一个汉字utf-8中使用三个字节表示
    def _string_hash(self, v):
        # A variable-length version of Python's builtin hash
        if v == "":
            return 0
        else:
            x = ord(v[0]) << 7   #第一个字节左移7位
            m = 1000003
            mask = 2 ** self.hashbits - 1
            for c in v:
                x = ((x * m) ^ ord(c)) & mask
            x ^= len(v)
            if x == -1:
                x = -2
            return x

    #与另一个simhash类比较
    def hamming_distance(self, other_hash):
        x = (self.hash ^ other_hash.hash) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    #与一个hash值比较
    def hamming_distance_with_val(self, other_hash_val):
        x = (self.hash ^ other_hash_val) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    def similarity(self, other_hash):
        b = self.hashbits
        return float(b - self.hamming_distance(other_hash)) / b

    def similarity_with_val(self, other_hash_val):
        b = self.hashbits
        return float(b - self.hamming_distance_with_val(other_hash_val)) / b


#从64位取字段,建立索引
def get_4_segments(hash_bits):
    fir = hash_bits & 0b11000000000000000000000000111111110000000000000000000000001111
    sec = hash_bits & 0b00111100000000000000001111000000001111000000000000000011110000
    thi = hash_bits & 0b00000011110000000011110000000000000000111100000000111100000000
    fou = hash_bits & 0b00000000001111111100000000000000000000000011111111000000000000
    fir2 = hash_bits & 0b11000000001111000000000000000011110000000000001111000000000000
    sec2 = hash_bits & 0b00111100000000111100000000000000001111000000000000111100000000
    thi2 = hash_bits & 0b00000011110000000011110000000000000000111100000000000011110000
    fou2 = hash_bits & 0b00000000001111000000001111000000000000000011110000000000001111
    return str(fir), str(sec), str(thi), str(fou), str(fir2), str(sec2), str(thi2), str(fou2)


def dif_bit(val1, val2, hashbits=64):
    x = (val1 ^ val2) & ((1 << hashbits) - 1)
    tot = 0
    while x:
        tot += 1
        x &= x - 1
    return tot