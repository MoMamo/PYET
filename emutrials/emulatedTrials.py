from utils_et.utils_et import list_contains
from utils_et.yaml_act import yaml_load
import os
import datetime

from CPRD.functions import tables, modalities, merge
import pyspark.sql.functions as F


def extract_first_diagnosis(diagnoses,cprd,hes,procedures,column_name='unnamed disease'):
    md1=MedicalDictionary.MedicalDictionaryRiskPrediction(file,spark)
    w=Window.partitionBy('patid').orderBy('eventdate')
    
    
    schema=StructType([StructField('patid', StringType(), True),
                       StructField('eventdate', DateType(), True),
                       StructField('code', StringType(), True),
                       StructField('source', StringType(), True)])
    
    medcodes=[]
    icdcodes=[]
    procedurecodes=[]
    condition_records_cprd=spark.sqlContext.createDataFrame(spark.sc.emptyRDD(),schema)
    condition_records_hes=spark.sqlContext.createDataFrame(spark.sc.emptyRDD(),schema)
    condition_records_procedures=spark.sqlContext.createDataFrame(spark.sc.emptyRDD(),schema)
    
    if diagnoses['source']=='existing phenotypes':
        for diagnosis in diagnoses['phenotypes']:
                medcodes+=md1.queryDisease(diagnosis)[diagnosis]['Medcode']
                icdcodes+=md1.queryDisease(diagnosis)[diagnosis]['ICD']
    elif diagnoses['source']=='self defined':
        medcodes+=diagnoses['phenotypes']['Medcode']
        icdcodes+=diagnoses['phenotypes']['ICD']
        procedurecodes=diagnoses['phenotypes']['OPCS']
    else:
        raise ValueError("The value entered for the parameter diagnoses['source'] is {}.\
        Use either 'existing phenotypes' or 'self defined' instead".format(diagnoses['source'])) 
        
        
    #find the first medcodes relating to the disease from cprd data
    if any(medcodes):
        condition_records_cprd=cprd.where(F.col('medcode').isin(medcodes))
        condition_records_cprd=condition_records_cprd.withColumn('row',F.row_number().over(w))\
                                              .where(F.col('row')==1)\
                                              .select(['patid','eventdate','Medcode'])\
                                              .withColumnRenamed('Medcode','code')\
                                              .withColumn('source',F.lit('cprd'))
        
    if any(icdcodes):
    #find the first ICD10 pertaining to the disease in HES
        rexp='|'.join(icdcodes)
        condition_records_hes=hes.where(F.col('ICD').rlike(rexp))
        condition_records_hes=condition_records_hes.withColumn('row',F.row_number().over(w))\
                                              .where(F.col('row')==1).select(['patid','eventdate','ICD'])\
                                              .withColumnRenamed('ICD','code')\
                                              .withColumn('source',F.lit('hes'))
    if any(procedurecodes):
    #find the first OPC code pertaining to the disease in Procedures
        condition_records_procedures=procedures.where(F.col('OPCS').isin(procedurecodes))
        condition_records_procedures=condition_records_procedures.withColumn('row',F.row_number().over(w))\
                                              .where(F.col('row')==1)\
                                              .select(['patid','eventdate','OPCS'])\
                                              .withColumnRenamed('OPCS','code')\
                                              .withColumn('source',F.lit('procedures'))
                    

    condition_records=condition_records_hes.union(condition_records_cprd).union(condition_records_procedures)

    condition_records=condition_records.withColumn('row',F.row_number().over(w))\
                                          .where(F.col('row')==1)\
                                          .withColumnRenamed('eventdate',column_name).cache()
    return condition_records             



def extract_all_diagnosis(diagnoses,cprd,hes,procedures,column_name='unnamed disease'):
    md1=MedicalDictionary.MedicalDictionaryRiskPrediction(file,spark)
    w=Window.partitionBy('patid').orderBy('eventdate')
    
    
    schema=StructType([StructField('patid', StringType(), True),
                       StructField('eventdate', DateType(), True),
                       StructField('code', StringType(), True),
                       StructField('source', StringType(), True)])
    
    medcodes=[]
    icdcodes=[]
    procedurecodes=[]
    condition_records_cprd=spark.sqlContext.createDataFrame(spark.sc.emptyRDD(),schema)
    condition_records_hes=spark.sqlContext.createDataFrame(spark.sc.emptyRDD(),schema)
    condition_records_procedures=spark.sqlContext.createDataFrame(spark.sc.emptyRDD(),schema)
    
    if diagnoses['source']=='existing phenotypes':
        for diagnosis in diagnoses['phenotypes']:
                medcodes+=md1.queryDisease(diagnosis)[diagnosis]['Medcode']
                icdcodes+=md1.queryDisease(diagnosis)[diagnosis]['ICD']
    elif diagnoses['source']=='self defined':
        medcodes+=diagnoses['phenotypes']['Medcode']
        icdcodes+=diagnoses['phenotypes']['ICD']
        procedurecodes=diagnoses['phenotypes']['OPCS']
    else:
        raise ValueError("The value entered for the parameter diagnoses['source'] is {}.\
        Use either 'existing phenotypes' or 'self defined' instead".format(diagnoses['source'])) 
        
        
    #find the first medcodes relating to the disease from cprd data
    if any(medcodes):
        condition_records_cprd=cprd.where(F.col('medcode').isin(medcodes))
        condition_records_cprd=condition_records_cprd.withColumn('row',F.row_number().over(w))\
                                              .select(['patid','eventdate','Medcode'])\
                                              .withColumnRenamed('Medcode','code')\
                                              .withColumn('source',F.lit('cprd'))
        
    if any(icdcodes):
    #find the first ICD10 pertaining to the disease in HES
        rexp='|'.join(icdcodes)
        condition_records_hes=hes.where(F.col('ICD').rlike(rexp))
        condition_records_hes=condition_records_hes.withColumn('row',F.row_number().over(w))\
                                              .select(['patid','eventdate','ICD'])\
                                              .withColumnRenamed('ICD','code')\
                                              .withColumn('source',F.lit('hes'))
    if any(procedurecodes):
    #find the first OPC code pertaining to the disease in Procedures
        condition_records_procedures=procedures.where(F.col('OPCS').isin(procedurecodes))
        condition_records_procedures=condition_records_procedures.withColumn('row',F.row_number().over(w))\
                                              .select(['patid','eventdate','OPCS'])\
                                              .withColumnRenamed('OPCS','code')\
                                              .withColumn('source',F.lit('procedures'))
                    

    condition_records=condition_records_hes.union(condition_records_cprd).union(condition_records_procedures)

    condition_records=condition_records.withColumn('row',F.row_number().over(w))\
                                          .cache()
    return condition_records             

def extract_first_diagnosis_from_dict(diagnoses_dict,clinical,hes,procedures):
    all_conditions=[]
    for col_name,diagnoses in diagnoses_dict.items():
        condition_records=extract_first_diagnosis(diagnoses,clinical,hes,procedures,col_name)

        if all_conditions:
            all_conditions=all_conditions.join(condition_records.select("patid",col_name),on='patid',how='outer').cache()
        else:
            all_conditions=condition_records.select("patid",col_name)
    return all_conditions



class EmulatedTrial():
    def __init__(self,trial_config:dict = None,demographic=None,spark=None):
        self.trial_config=trial_config
        self.trial_predictors=self.trial_config['data_extraction_params']['predictors']
        self.spark=spark
        self.demographic=demographic
        self.trial_predictors_path=trial_config['path_params']['predictors_paths']
        self.predictors_paths_dict=None
        
    def set_trial_predictors_path(self,trial_predictors_path):
        self.trial_predictors_path=trial_predictors_path
    
    def generate_path2predictors(self):
        temp_path=self.trial_predictors_path
        walk=[v for v in os.walk(temp_path)]
        predictor_paths=[drct[0] for drct in walk if (drct[0].split("/.")[-1]!="ipynb_checkpoints") & (drct[0].split("/")[-1]!=temp_path.split("/")[-1]) & (drct[0].split("/")[-1]!="Meds")]
        predictor_paths={predictor.split("/")[-1]:predictor for predictor in predictor_paths}
        self.predictors_paths_dict={predictor:predictor_path+"/"+file for predictor,predictor_path in predictor_paths.items() for file in os.listdir(predictor_path) if file.endswith('.csv')}
        print("predictors_paths_dict attribute is now set. Check before running emulatedtrials.")
        
        
    def __check_predictors_validity(self):
        valid_predictors=yaml_load("./utils_et/current_predictors.yaml")['predictors']
        cond1=list_contains(self.trial_config['data_extraction_params']['predictors'],valid_predictors) 
        cond2=True
        if ('extract_first_diagnosis_from_yaml' in self.trial_config['data_extraction_params']['predictors']) | ('extract_all_diagnosis_from_yaml' in self.trial_config['data_extraction_params']['predictors']):
            cond2=list_contains(self.trial_config['data_extraction_params']['first_diagnosis']+trial_config['data_extraction_params']['history_diagnosis'],valid_predictors)
        return cond1 & cond2

        
       
    def extract_predictors(self):
        dir_path=self.trial_config['path_params']['predictors_paths']
        data_extraction_duration=self.trial_config['data_extraction_params']['duration']
        phenotypes_dict=yaml_load(self.trial_config["path_params"]['phenotypes_paths'])
        demographic=self.demographic
        
        if not self.__check_predictors_validity():
            preds=set(self.trial_config['data_extraction_params']['predictors'])-set(self.trial_predictors)
            raise ValueError("Currently the implemented predictors are: "+str(self.trial_predictors)+ " you have included "+str(preds)+' in the trial config file. Alternatively check if all requested predictors have been defined.')
            
        
        
        if not os.path.exists(dir_path):
            os.makedirs(dir_path) 
            print("The new directory is created!")
            
        #READ THE MAIN TABLES    
        file=yaml_load(self.trial_config['path_params']['cprd_package_paths']+'/config/config.yaml')['file_path']
        clinical = tables.retrieve_clinical(dir=file['clinical'], spark=self.spark)\
        .select(['patid','eventdate','medcode'])\
        .join(demographic,on='patid',how="leftsemi")\
        .where((F.col('eventdate')>datetime.date(int(data_extraction_duration[0].split('-')[0]),int(data_extraction_duration[0].split('-')[1]),int(data_extraction_duration[0].split('-')[2])))\
               & (F.col('eventdate')<datetime.date(int(data_extraction_duration[1].split('-')[0]),int(data_extraction_duration[1].split('-')[1]),int(data_extraction_duration[1].split('-')[2]))))\
        .cache()

        procedures = modalities.retrieve_procedure(file, self.spark, duration=(int(data_extraction_duration[0].split('-')[0]), int(data_extraction_duration[1].split('-')[0])), demographics=demographic, start_col='startdate', end_col='enddate').cache()

        hes = tables.retrieve_hes_diagnoses(dir=file['diagnosis_hes'], spark=self.spark)\
        .select(['patid','eventdate','ICD','ICDx'])\
        .join(demographic,on='patid',how="leftsemi")\
        .where((F.col('eventdate')>datetime.date(int(data_extraction_duration[0].split('-')[0]),int(data_extraction_duration[0].split('-')[1]),int(data_extraction_duration[0].split('-')[2])))\
               & (F.col('eventdate')<datetime.date(int(data_extraction_duration[1].split('-')[0]),int(data_extraction_duration[1].split('-')[1]),int(data_extraction_duration[1].split('-')[2]))))\
        .cache()

        hes=hes.withColumn('ICDndot',F.translate(F.col("ICD"),'.','')).cache()
        ###################
        for predictor in self.trial_config['data_extraction_params']['predictors']:
            if predictor=="Smoking":
                smoking=modalities.retrieve_smoking_status(file,self.spark)
                smoking=smoking.join(demographic,on='patid',how='leftsemi')
                smoking.coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+predictor)
            if predictor=="BMI":
                bmi=modalities.retrieve_bmi(file, self.spark).\
                join(demographic,on='patid',how="leftsemi")
                bmi.select(['patid','eventdate','BMI']).coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+predictor)
            if predictor =="SBP":
                sys_bp=modalities.retrieve_systolic_bp_measurement(file, self.spark).\
                join(demographic,on='patid',how="leftsemi")
                sys_bp.coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+predictor)
            if predictor == "HR":
                hr = modalities.retrieve_by_enttype(file, spark, enttype=131).\
                join(demographic,on='patid',how="leftsemi")
                hr.coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+predictor)
            if predictor=="Alcohol":
                alcohol=modalities.retrieve_drinking_status(file, self.spark).join(demographic,on='patid',how="leftsemi")
                alcohol.coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+predictor)
            if predictor=='NYHA_class':
                nyhaclasses=phenotypes_dict["heart_failure"]["nyha_class"]#class I, II, III & IV
                nyha=clinical.where(F.col("medcode").isin(nyhaclasses))
                nyha.select(['patid','eventdate','medcode']).coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+predictor)
            if predictor=='HF_subtypes':
                dict_hf_type_read=yaml_load(self.trial_config['path_params']['phenotypes_paths'])['heart_failure']['hf_subtype_med']
                dict_hf_type_icd=yaml_load(self.trial_config['path_params']['phenotypes_paths'])['heart_failure']['hf_subtype_ICD10']
                
                #grabs rows in the cohorts' GP and hes data that are part of the med\ICD10 codes and map them to HF types
                hf_types_read=clinical.filter(F.col('medcode').isin(list(dict_hf_type_read.keys())))\
                .replace(to_replace=dict_hf_type_read, subset=['medcode'])\
                .withColumnRenamed('medcode','HF type')
                hf_types_icd=hes.filter(F.col('ICD').isin(list(dict_hf_type_icd.keys()))).replace(to_replace=dict_hf_type_icd, subset=['ICD'])\
                .select(['patid','eventdate','ICD']).withColumnRenamed('ICD','HF type')
                
                # now merge the two group by patient and order based on event date, if a patient has HF-REF, get rid of everything else after, so for each patient,
                #we may have one of the following possible sequences (HF-UNS,HF-UNS->HF-REF, HF-REF)
                hf_types_read=hf_types_read.withColumn("source",F.lit('CPRD'))
                hf_types_icd=hf_types_icd.withColumn("source",F.lit('HES'))
                hf_types=hf_types_read.union(hf_types_icd)
                
                w=Window.partitionBy('patid').orderBy('eventdate')
                w2=Window.partitionBy('patid','change').orderBy('eventdate')

                #identify the first entry, if the first entry is HF-UNS and it changes to HF-REF, assign True to the column change,
                #take the first entry after grouping by patid and change which will give the first HF-UNS and HF-REF entry for each patient.
                hf_types=hf_types.withColumn("row",F.row_number().over(w)) \
                .withColumn("first_entry",F.first('HF type').over(w)) \
                .withColumn('change',F.when(((F.col('HF type')!=F.col('first_entry')) & (F.col('first_entry')=='HF-UNS') ),True)) \
                .where((F.col('change')==True) | (F.col('row')==1)) \
                .withColumn('rank',F.row_number().over(w2)) \
                .where(F.col('rank')==1) \
                .drop("first_entry").drop("change").drop("rank").drop("row") \
                .cache()

                hf_types.select(['patid','eventdate','HF type']).coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/hftype")
            if predictor=="first_diagnoses":
                phenotypes_dict2=yaml.load(open('./configs/additional_phenotypes.yaml'), Loader=yaml.Loader)
                conditions_dict={k:phenotypes_dict2[k] for k in self.trial_config['data_extraction_params']['first_diagnosis']}
                all_conditions=extract_first_diagnosis_from_dict(conditions_dict,clinical,hes,procedures)
                all_conditions.coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+predictor)
            if predictor=="history_diagnoses":
                phenotypes_dict2=yaml.load(open('./configs/additional_phenotypes.yaml'), Loader=yaml.Loader)
                conditions_dict={k:phenotypes_dict2[k] for k in self.trial_config['data_extraction_params']+"/"+predictor}
                for condition in conditions_dict.keys():
                    tempsdf=extract_all_diagnosis(conditions_dict[condition],clinical,hes,procedures,condition)
                    tempsdf.coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/"+condition.replace(' ','_').lower())
            if predictor=='lab_tests':
                lab_test=modalities.retrieve_lab_test(file, self.spark, 
                                      duration=(int(data_extraction_duration[0].split('-')[0]), int(data_extraction_duration[1].split('-')[0])), 
                                      demographics=demographic, start_col='startdate', end_col='enddate')\
                .select(['patid','eventdate','data2','data3']).withColumn("data2",F.col('data2').cast('double')).cache()
                extract_lab_measurements(self.trial_config['data_extraction_params']['lab_tests'],lab_test, self.trial_config)
            if predictor =='medications':
                therapy = tables.retrieve_therapy(dir=file['therapy'], spark=self.spark).select(['patid', 'prodcode', 'eventdate', 'bnfcode']).join(demographic.select('patid'),on="patid",how='inner').cache()
                drug_phenotypes=yaml_load("./configs/drug_phenotypes.yaml")
                crossmap = tables.retrieve_bnf_prod_crossmap(dir=file['prod2bnf'], spark=self.spark)
                therapy=merge.bnf_mapping(crossmap=crossmap, therapy=therapy).cache()
                med_list=self.trial_config['data_extraction_params']["medications"]
                if not list_contains(med_list,list(drug_phenotypes.keys())):
                    preds=set(med_list)-set(list(drug_phenotypes.keys()))
                    raise ValueError("Currently the defined drug phenotypes in configs/drug_phenotypes.yaml are: "+str(list(drug_phenotypes.keys()))+ " you have included "+str(preds)+' in the trial config file.')
                
                for medication in med_list:
                    X=therapy.where((F.col("bnfcode").isin(drug_phenotypes[medication]["bnf_code_list"]) & (F.col("prodcode").isin(drug_phenotypes[medication]["prodcode"]))))
                    X.coalesce(1).write.option("header",True).csv(self.trial_config['path_params']['predictors_paths']+"/Meds/"+medication)
                    
                        