import pyspark.sql.functions as F
import os

def make_dict_from_table(sdf,key,value='read'):
    df=sdf.groupBy(key).agg(F.collect_list(F.col(value)).alias("value_list")).cache().toPandas().set_index(key)

    df=df.to_dict()['value_list']
    return df

def list_contains(lista,listb):
    return set(lista).issubset(set(listb))
