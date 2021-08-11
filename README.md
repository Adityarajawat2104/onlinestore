There are 4 python files with code:
1. ingest_data - used to read raw data, refine raw data and save it.
2. tranform - where the tranformation logic is written
3. spark_functions - Some common functions and UDFs are defined in the file
4. recipe_pipeline - file which have the main function and run_pileline method to run the pipeline

Workflow:
Step1 - Raw data is read from input folder and refined with below logic:
	- Remove records where prep time and cook time both are are blank as this will not be valid use case
	- select only required columns - description, prepTime, cookTime

Step2 - refined data is saved in output/curated folder in parquet format for further processing.

Step3 - refined data from Step2 is used to perform the transformation logic as below:
	-UDF calculate_time is used to get time in minutes for prepTime and cookTime in new column cook_time_mins and prep_time_mins respectively
	-addition of cook_time_mins and prep_time_mins is stored in new column avg_total_cooking_time.
	-UDF calculate_complexity to calculate complexity category based on avg_total_cooking_time in new column difficulty
	
Step4 - Output of step3 is stored in csv format in output/processed folder

Other files:
1. Schema for raw data is stored in schemas folder and used to when reading raw data to save processing.
2. Logs folder have log files 
3. output/curated - where intermediate data is stored parquet format
4. output/processed - final output is stored in csv format 

