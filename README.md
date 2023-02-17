# PYET
Emulated trials python package for CPRD

Trial emulation is a powerful technique for causal inference on observational data (Danaei 2011). This ongoing work aims to make trial emulation accessible to CPRD users. If you are interesting in collaborating, please get in touch. 
The idea is to simplify a fairly complex analysis pipeline into a few steps. 

```python
et=EmulatedTrial(trial_config,demographic,spark)
et.extract_predictors() #or et.set_trial_predictors_path(PATH)
et.emulatetrials() # or et.set_trial_results_path(PATH)
et.analyse(method='cox_iptw',subgroups='labels')
```
1. create an instance of the class EmulatedTrial with the attributes 
  * trial_config: path to trial configuration yaml file
  * demographic: a pyspark dataframe with the cohort ids. E.g. A cohort aged between 35 and 70 with prevalent diabetes and heart failure. 
  * spark initialisation object 
  
2. Extract predictors as listed in the trial_config file and save them in a predefined location. This is necessary to avoid expenssive conditional searches during trial emulation. Alternatively if the predictors have already been extracted, use self.set_trial_predictors_path(PATH) 

3. Emulate a series of trials based on the parameters defined in the trial_config file and save covariates, treatment status, outcome etc in a tidy format. If these are previously extracted use self.set_trial_results_path(PATH).

4. analyse the trial data using the specified method and for different subgroups. 

1&2 are currently implemented. 3&4 are currently in seperate Jupyter Notebooks and need to be incorporated in the EmulatedTrial class.
 
# Acknowledgement
This work is supported by the Deep Medicine programme, University of Oxford. The to-be package makes extensive use of the CPRD package developed by
@srn284 and @yikuanli
 
#Collaborators
[Your name goes here]

# References
Danaei G, Rodríguez LA, Cantero OF, Logan R, Hernán MA. Observational data for comparative effectiveness research: an emulation of randomised trials of statins and primary prevention of coronary heart disease. Stat Methods Med Res. 2013 Feb;22(1):70-96. doi: 10.1177/0962280211403603. Epub 2011 Oct 19. PMID: 22016461; PMCID: PMC3613145.
