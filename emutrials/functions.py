import pyspark.sql.functions as F

def _extract_lab_measurement(measurement,lab_test):
    entype_code=measurement['entity_number']
    all_measured=lab_test.filter(F.col('enttype')==entype_code).cache()

    if any(list(measurement["valid_units_codes"].keys())):
        all_measured=all_measured.filter(F.col('data3').isin(list(measurement['valid_units_codes'].keys())))
    #create a expression for transformations
    if any([transformation!='None' for transformation in list(measurement["valid_units_codes"].values())]):
        string_expression=["WHEN data3 = '{}' THEN data2{} ".format(k,v) for k,v in measurement["valid_units_codes"].items() if v!="None"]
        string_expression='CASE '+''.join(string_expression)+'ELSE data2 END'
        print(string_expression)

        all_measured=all_measured.filter(F.col('data3').isin(list(measurement['valid_units_codes'].keys()))).withColumn('data2',F.expr(string_expression))
    if measurement["mesurement_range"]:
        all_measured=all_measured.filter((F.col('data2')<measurement["mesurement_range"][1]) | (F.col('data2')>measurement["mesurement_range"][0]))
    return all_measured

def log_measurement_summary(measurement,lab_test,verbose=0):
    mes_unit=read_txt(spark.sc,spark.sqlContext, cprd_config["file_path"]['entity'].rsplit('/',1)[0]+'/TXTFILES/SUM.txt').toPandas()
    mes_unit=mes_unit.set_index('Code').T.to_dict('list')
    mes_unit={k:v[0] for k,v in mes_unit.items()}
    
    FORMAT = '%(asctime)s %(logtype)s %(message)s'
    logger=logging.getLogger("LabMesExtractionLog")
    logger.setLevel(logging.DEBUG)
    fhandler = logging.FileHandler(filename=os.getcwd()+"/logs/LabMesExtractionLog{}.log".format(date.today().strftime("_%d_%m_%Y")), mode='w')
    logger.addHandler(fhandler)

    entype_code=measurement['entity_number']
    all_measured=lab_test.filter(F.col('enttype')==entype_code).cache()

    sum_of_codes=all_measured.select(['data2','data3']).groupBy('data3').count().withColumnRenamed('count','cnt_per_group')\
    .withColumn('perc_of_count_total', (F.col('cnt_per_group') / all_measured.count()) * 100 )

    codes_list=sum_of_codes.select("data3").rdd.map(lambda x: x[0]).collect()
    codes_dict={code:mes_unit[code] for code in codes_list}

    sum_of_codes=sum_of_codes.withColumn("measure_unit",F.col('data3')).replace(codes_dict,subset=["measure_unit"])

    summary_table=all_measured.select(['data2','data3']).groupBy('data3').mean().join(sum_of_codes,on="data3",how='left')

    #Logging
    logger.info('Extracting values for the predictors with entity number {} \n'.format(measurement['entity_number']))
    logger.info('\n {} \n'.format(summary_table.toPandas().to_markdown()))

    if verbose!=0:
        print('logged with Entity number {}'.format(measurement['entity_number']))
        summary_table.show()

def extract_lab_measurements(measurement_list,lab_test,trial_config,logging=False):
    for measurement_name,measurement_dict in measurement_list.items():
        print('Extracting lab measurement: '+measurement_name)
        if logging==True:
            log_measurement_summary(measurement,lab_test,verbose=0)
        all_measured=_extract_lab_measurement(measurement_dict,lab_test)
        all_measured.select(['patid','eventdate','data2']).withColumnRenamed('data2',measurement_name).coalesce(1).write.option("header",True).csv(trial_config['path_params']['predictors_paths']+"/"+measurement_name)