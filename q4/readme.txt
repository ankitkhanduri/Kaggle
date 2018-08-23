Additional libraries used 

import kaggle
import subprocess
import os
import pandas as pd
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import pymysql
import nltk
import numpy as np
from pyspark import SparkConf, SparkContext
from difflib import SequenceMatcher
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH

