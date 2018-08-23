External libraries used 

import kaggle
import subprocess
import os
import pandas as pd
import numpy as np
from numpy import median
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import pymysql
from dateutil.relativedelta import relativedelta
import time
from scipy import stats

