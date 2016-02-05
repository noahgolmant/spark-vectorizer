#!/usr/bin/python -tt
# -*- coding: utf-8 -*-
import newspaper
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import string
import sys

stemmer = SnowballStemmer('english')
stop = stopwords.words('english')

data = open('text.csv', 'w')
with open('db.csv', 'r') as f:
    lines = f.readlines()
    i = 1
    end = len(lines)
    print "Total: {}".format(end)
    for line in lines:
        try:
            title, link = line.split(',')
            news = newspaper.Article(link)
            news.download()
            news.parse()
        except:
            continue
        sys.stdout.write(str(i)+',')
        i += 1
        text = ''.join([c for c in news.text if c not in string.punctuation])
        text = [w.lower() for w in text.split()]
        text = [stemmer.stem(w) for w in text if w not in stop]
        if len(text) > 1:
            data.write(title+ "," + ','.join(text).encode('utf-8') + "\n")
data.close()
        
