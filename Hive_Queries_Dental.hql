-- View existing tables
SHOW tables;

-- Print column headers
SET hive.cli.print.header=true;

-- Create a table holding the reduced number of columns
CREATE TABLE loaded_dental_plans (
		State_Code	String	
	, 	County_Name	String
	,	Issuer_Name	String
	,   Plan_ID		String
	,	Child_Only_Offering	String
	,	Routine_Dental_Services_Adult_Coverage	int
	,	Basic_Dental_Care_Adult_Coverage      	int
	,	Major_Dental_Care_Adult_Coverage      	int
	,	Orthodontia_Adult_Coverage				int
	,	Dental_Check_Up_For_Children_Coverage 	int
	,	Basic_Dental_Care_Child_Coverage      	int
	,	Major_Dental_Care_Child_Coverage      	int
	,	Orthodontia_Child_Coverage				int
	,	Premium_Child_Age_U14					float
	,	Premium_Child_Age_18 					float
	,	Premium_Adult_Individual_Age_21			float
	,	Premium_Adult_Individual_Age_27			float
	,	Premium_Adult_Individual_Age_30			float
	,	Premium_Adult_Individual_Age_40			float
	,	Premium_Adult_Individual_Age_50			float
	,	Premium_Adult_Individual_Age_60         float
	,	Premium_Couple_21                       float
	,	Premium_Couple_30						float
	,	Premium_Couple_40                       float 
	,	Premium_Couple_50                              float 
	,	Premium_Couple_60                              float 
	,	Couple_1_Child_Age_21                          float 
	,	Couple_1_Child_Age_30                          float 
	,	Couple_1_Child_Age_40                          float 
	,	Couple_1_Child_Age_50                          float 
	,	Couple_2_Children_Age_21                       float 
	,	Couple_2_Children_Age_30                       float 
	,	Couple_2_Children_Age_40                       float 
	,	Couple_2_Children_Age_50                       float 
	,	Couple_3_Plus_Children_Age_21                  float 
	,	Couple_3_Plus_Children_Age_30                  float 
	,	Couple_3_Plus_Children_Age_40                  float 
	,	Couple_3_Plus_Children_Age_50                  float 
	,	Individual_1_Child_Age_21                      float 
	,	Individual_1_Child_Age_30                      float 
	,	Individual_1_Child_Age_40                      float 
	,	Individual_1_Child_Age_50                      float 
	,	Individual_2_Children_Age_21                   float 
	,	Individual_2_Children_Age_30                   float 
	,	Individual_2_Children_Age_40                   float 
	,	Individual_2_Children_Age_50                   float 
	,	Individual_3_Plus_Children_Age_21              float 
	,	Individual_3_Plus_Children_Age_30              float 
	,	Individual_3_Plus_Children_Age_40              float 
	,	Individual_3_Plus_Children_Age_50              float 
	,	Dental_Deductible_Individual                   float 
	,	Dental_Deductible_Family                       float 
	,	Dental_Deductible_Family_Per_Person            float 
	,	Dental_Maximum_Out_Of_Pocket_Individual        float 
	,	Dental_Maximum_Out_Of_Pocket_Family            float 
	,	Dental_Maximum_Out_Of_Pocket_Family_Per_Person float 
	,	Routine_Dental_Services_Adult                  float 
	,	Basic_Dental_Care_Adult                        float 
	,	Major_Dental_Care_Adult                        float 
	,	Orthodontia_Adult                              float 
	,	Dental_Check_Up_For_Children                   float 
	,	Basic_Dental_Care_Child                        float 
	,	Major_Dental_Care_Child                        float 
	,	Orthodontia_Child                              float 
	,	Customer_Service_Phone_Number_Local            String
	,	Network_URL                                    String
	,	Plan_Brochure_URL                              String
	,	Summary_of_Benefits_URL                        String) 
row format delimited 
fields terminated by ',' 
location 'gs://dataproc-staging-europe-west3-341198046822-nuvlyckm/loaded_dental_plans';

-- Review new table
DESCRIBE loaded_dental_plans;

