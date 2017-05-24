# -*- coding: utf-8 -*-
# @Time    : 17/5/24 下午4:13
# @Author  : liulei
# @Brief   : 处理新闻标题和缩略图hash, 用来排重
# @File    : img_title_hash.py
# @Software: PyCharm Community Edition

from PIL import Image

EXTS = ('jpg', 'jpeg', 'JPG', 'JPEG', 'gif', 'GIF', 'png', 'PNG')



def avhash(im):
    if not isinstance(im, Image.Image):
        im = Image.open(im)
    im = im.resize((8, 8), Image.ANTIALIAS).convert('L')
    avg = reduce(lambda x, y: x + y, im.getdata()) / 64.
    return reduce(lambda x, (y, z): x | (z << y),
                  enumerate(map(lambda i: 0 if i < avg else 1, im.getdata())),
                  0)


def hamming(h1, h2):
    h, d = 0, h1 ^ h2
    while d:
        h += 1
        d &= d - 1
    return h


if __name__ == "__main__":
    h1 = avhash('6afc41d15956690f0f762c93abbc20cfb626c72f8c526f8fafa9ccbe91aa4eac.jpg')
    h2 = avhash('e13d12e83c228db342cc3dc7c35d2c21803d457bc9ab16d654b67b314c80dd3f.jpg')
    print hamming(h1, h2)

