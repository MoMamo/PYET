#!/usr/bin/env python
# coding: utf-8

# In[1]:


def make_dict_from_table(sdf,key,value='read'):
    df=sdf.groupBy(key).agg(F.collect_list(F.col(value)).alias("value_list")).cache().toPandas().set_index(key)

    df=df.to_dict()['value_list']
    return df

