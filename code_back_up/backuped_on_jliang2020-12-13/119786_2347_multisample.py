import datetime
import pandas as pd
import numpy as np

from multiprocessing import Pool
from functools import partial



def getprogramviewers_readonly(f,programlist,targetcolumns):
    try:
        df = pd.read_csv(f,sep=',',header=None, compression='gzip',dtype='str',
                            usecols = targetcolumns)
        df = df[(df[4].isin(programlist))]
    except:
            logging.info("0")
    return df

def getprogram_multy(programlist,targetcolumns,filepath):
    pool = Pool(30)
    result = pool.map(
        partial(getprogramviewers_readonly,
                programlist = programlist,
                targetcolumns = targetcolumns),filepath)
    pool.close()
    pool.join()
    dfall = pd.DataFrame()
    for res in result:
        if res is not None:
            dfall = dfall.append(res)
    return dfall

#
