--Load data
dentalPlans = LOAD 'gs://dataproc-staging-europe-west3-341198046822-nuvlyckm/dental_plan_data.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER')  
AS(State_Code:chararray,FIPS_County_Code:chararray,County_Name:chararray,Metal_Level:chararray,Issuer_Name:chararray,HIOS_Issuer_ID:chararray,Plan_ID:chararray,Plan_Marketing_Name:chararray,Plan_Type:chararray,Rating_Area:chararray,Child_Only_Offering:chararray,Source:chararray,Customer_Service_Phone_Number_Local:chararray,Customer_Service_Phone_Number_Toll_Free:chararray,Customer_Service_Phone_Number_TTY:chararray,Network_URL:chararray,Plan_Brochure_URL:chararray,Summary_of_Benefits_URL:chararray,Drug_Formulary_URL:chararray,Routine_Dental_Services_Adult_Coverage:chararray,Basic_Dental_Care_Adult_Coverage:chararray,Major_Dental_Care_Adult_Coverage:chararray,Orthodontia_Adult_Coverage:chararray,Dental_Check_Up_For_Children_Coverage:chararray,Basic_Dental_Care_Child_Coverage:chararray,Major_Dental_Care_Child_Coverage:chararray,Orthodontia_Child_Coverage:chararray,Premium_Rates:chararray,Premium_Child_Age_U14:chararray,Premium_Child_Age_18:chararray,Premium_Adult_Individual_Age_21:chararray,Premium_Adult_Individual_Age_27:chararray,Premium_Adult_Individual_Age_30:chararray,Premium_Adult_Individual_Age_40:chararray,Premium_Adult_Individual_Age_50:chararray,Premium_Adult_Individual_Age_60:chararray,Premium_Couple_21:chararray,Premium_Couple_30:chararray,Premium_Couple_40:chararray,Premium_Couple_50:chararray,Premium_Couple_60:chararray,Couple_1_Child_Age_21:chararray,Couple_1_Child_Age_30:chararray,Couple_1_Child_Age_40:chararray,Couple_1_Child_Age_50:chararray,Couple_2_Children_Age_21:chararray,Couple_2_Children_Age_30:chararray,Couple_2_Children_Age_40:chararray,Couple_2_Children_Age_50:chararray,Couple_3_Plus_Children_Age_21:chararray,Couple_3_Plus_Children_Age_30:chararray,Couple_3_Plus_Children_Age_40:chararray,Couple_3_Plus_Children_Age_50:chararray,Individual_1_Child_Age_21:chararray,Individual_1_Child_Age_30:chararray,Individual_1_Child_Age_40:chararray,Individual_1_Child_Age_50:chararray,Individual_2_Children_Age_21:chararray,Individual_2_Children_Age_30:chararray,Individual_2_Children_Age_40:chararray,Individual_2_Children_Age_50:chararray,Individual_3_Plus_Children_Age_21:chararray,Individual_3_Plus_Children_Age_30:chararray,Individual_3_Plus_Children_Age_40:chararray,Individual_3_Plus_Children_Age_50:chararray,Standard_On_Exchange:chararray,Dental_Deductible_Individual:chararray,Dental_Deductible_Family:chararray,Dental_Deductible_Family_Per_Person:chararray,Dental_Maximum_Out_Of_Pocket_Individual:chararray,Dental_Maximum_Out_Of_Pocket_Family:chararray,Dental_Maximum_Out_Of_Pocket_Family_Per_Person:chararray,Routine_Dental_Services_Adult:chararray,Basic_Dental_Care_Adult:chararray,Major_Dental_Care_Adult:chararray,Orthodontia_Adult:chararray,Dental_Check_Up_For_Children:chararray,Basic_Dental_Care_Child:chararray,Major_Dental_Care_Child:chararray,Orthodontia_Child:chararray);

final_output = foreach dentalPlans generate State_Code
				, 	County_Name
				,	Issuer_Name
				,   Plan_ID
				,	Child_Only_Offering				
				,	(( Routine_Dental_Services_Adult_Coverage == '') ? 0:1 ) AS Routine_Dental_Services_Adult_Coverage
				,	(( Basic_Dental_Care_Adult_Coverage       == '') ? 0:1 ) AS Basic_Dental_Care_Adult_Coverage      
				,	(( Major_Dental_Care_Adult_Coverage       == '') ? 0:1 ) AS Major_Dental_Care_Adult_Coverage      
				,	(( Orthodontia_Adult_Coverage             == '') ? 0:1 ) AS Orthodontia_Adult_Coverage            
				,	(( Dental_Check_Up_For_Children_Coverage  == '') ? 0:1 ) AS Dental_Check_Up_For_Children_Coverage 
				,	(( Basic_Dental_Care_Child_Coverage       == '') ? 0:1 ) AS Basic_Dental_Care_Child_Coverage      
				,	(( Major_Dental_Care_Child_Coverage       == '') ? 0:1 ) AS Major_Dental_Care_Child_Coverage      
				,	(( Orthodontia_Child_Coverage             == '') ? 0:1 ) AS Orthodontia_Child_Coverage
				,	(float) REPLACE ((( Premium_Child_Age_U14                           == '') ? '0': Premium_Child_Age_U14                          ), '\\\$','' ) AS Premium_Child_Age_U14
				,	(float) REPLACE ((( Premium_Child_Age_18                            == '') ? '0': Premium_Child_Age_18                           ), '\\\$','' ) AS Premium_Child_Age_18                          
				,	(float) REPLACE ((( Premium_Adult_Individual_Age_21                 == '') ? '0': Premium_Adult_Individual_Age_21                ), '\\\$','' ) AS Premium_Adult_Individual_Age_21               
				,	(float) REPLACE ((( Premium_Adult_Individual_Age_27                 == '') ? '0': Premium_Adult_Individual_Age_27                ), '\\\$','' ) AS Premium_Adult_Individual_Age_27               
				,	(float) REPLACE ((( Premium_Adult_Individual_Age_30                 == '') ? '0': Premium_Adult_Individual_Age_30                ), '\\\$','' ) AS Premium_Adult_Individual_Age_30               
				,	(float) REPLACE ((( Premium_Adult_Individual_Age_40                 == '') ? '0': Premium_Adult_Individual_Age_40                ), '\\\$','' ) AS Premium_Adult_Individual_Age_40               
				,	(float) REPLACE ((( Premium_Adult_Individual_Age_50                 == '') ? '0': Premium_Adult_Individual_Age_50                ), '\\\$','' ) AS Premium_Adult_Individual_Age_50               
				,	(float) REPLACE ((( Premium_Adult_Individual_Age_60                 == '') ? '0': Premium_Adult_Individual_Age_60                ), '\\\$','' ) AS Premium_Adult_Individual_Age_60               
				,	(float) REPLACE ((( Premium_Couple_21                               == '') ? '0': Premium_Couple_21                              ), '\\\$','' ) AS Premium_Couple_21                             
				,	(float) REPLACE ((( Premium_Couple_30                               == '') ? '0': Premium_Couple_30                              ), '\\\$','' ) AS Premium_Couple_30                             
				,	(float) REPLACE ((( Premium_Couple_40                               == '') ? '0': Premium_Couple_40                              ), '\\\$','' ) AS Premium_Couple_40                             
				,	(float) REPLACE ((( Premium_Couple_50                               == '') ? '0': Premium_Couple_50                              ), '\\\$','' ) AS Premium_Couple_50                             
				,	(float) REPLACE ((( Premium_Couple_60                               == '') ? '0': Premium_Couple_60                              ), '\\\$','' ) AS Premium_Couple_60                             
				,	(float) REPLACE ((( Couple_1_Child_Age_21                           == '') ? '0': Couple_1_Child_Age_21                          ), '\\\$','' ) AS Couple_1_Child_Age_21                         
				,	(float) REPLACE ((( Couple_1_Child_Age_30                           == '') ? '0': Couple_1_Child_Age_30                          ), '\\\$','' ) AS Couple_1_Child_Age_30                         
				,	(float) REPLACE ((( Couple_1_Child_Age_40                           == '') ? '0': Couple_1_Child_Age_40                          ), '\\\$','' ) AS Couple_1_Child_Age_40                         
				,	(float) REPLACE ((( Couple_1_Child_Age_50                           == '') ? '0': Couple_1_Child_Age_50                          ), '\\\$','' ) AS Couple_1_Child_Age_50                         
				,	(float) REPLACE ((( Couple_2_Children_Age_21                        == '') ? '0': Couple_2_Children_Age_21                       ), '\\\$','' ) AS Couple_2_Children_Age_21                      
				,	(float) REPLACE ((( Couple_2_Children_Age_30                        == '') ? '0': Couple_2_Children_Age_30                       ), '\\\$','' ) AS Couple_2_Children_Age_30                      
				,	(float) REPLACE ((( Couple_2_Children_Age_40                        == '') ? '0': Couple_2_Children_Age_40                       ), '\\\$','' ) AS Couple_2_Children_Age_40                      
				,	(float) REPLACE ((( Couple_2_Children_Age_50                        == '') ? '0': Couple_2_Children_Age_50                       ), '\\\$','' ) AS Couple_2_Children_Age_50                      
				,	(float) REPLACE ((( Couple_3_Plus_Children_Age_21                   == '') ? '0': Couple_3_Plus_Children_Age_21                  ), '\\\$','' ) AS Couple_3_Plus_Children_Age_21                 
				,	(float) REPLACE ((( Couple_3_Plus_Children_Age_30                   == '') ? '0': Couple_3_Plus_Children_Age_30                  ), '\\\$','' ) AS Couple_3_Plus_Children_Age_30                 
				,	(float) REPLACE ((( Couple_3_Plus_Children_Age_40                   == '') ? '0': Couple_3_Plus_Children_Age_40                  ), '\\\$','' ) AS Couple_3_Plus_Children_Age_40                 
				,	(float) REPLACE ((( Couple_3_Plus_Children_Age_50                   == '') ? '0': Couple_3_Plus_Children_Age_50                  ), '\\\$','' ) AS Couple_3_Plus_Children_Age_50                 
				,	(float) REPLACE ((( Individual_1_Child_Age_21                       == '') ? '0': Individual_1_Child_Age_21                      ), '\\\$','' ) AS Individual_1_Child_Age_21                     
				,	(float) REPLACE ((( Individual_1_Child_Age_30                       == '') ? '0': Individual_1_Child_Age_30                      ), '\\\$','' ) AS Individual_1_Child_Age_30                     
				,	(float) REPLACE ((( Individual_1_Child_Age_40                       == '') ? '0': Individual_1_Child_Age_40                      ), '\\\$','' ) AS Individual_1_Child_Age_40                     
				,	(float) REPLACE ((( Individual_1_Child_Age_50                       == '') ? '0': Individual_1_Child_Age_50                      ), '\\\$','' ) AS Individual_1_Child_Age_50                     
				,	(float) REPLACE ((( Individual_2_Children_Age_21                    == '') ? '0': Individual_2_Children_Age_21                   ), '\\\$','' ) AS Individual_2_Children_Age_21                  
				,	(float) REPLACE ((( Individual_2_Children_Age_30                    == '') ? '0': Individual_2_Children_Age_30                   ), '\\\$','' ) AS Individual_2_Children_Age_30                  
				,	(float) REPLACE ((( Individual_2_Children_Age_40                    == '') ? '0': Individual_2_Children_Age_40                   ), '\\\$','' ) AS Individual_2_Children_Age_40                  
				,	(float) REPLACE ((( Individual_2_Children_Age_50                    == '') ? '0': Individual_2_Children_Age_50                   ), '\\\$','' ) AS Individual_2_Children_Age_50                  
				,	(float) REPLACE ((( Individual_3_Plus_Children_Age_21               == '') ? '0': Individual_3_Plus_Children_Age_21              ), '\\\$','' ) AS Individual_3_Plus_Children_Age_21             
				,	(float) REPLACE ((( Individual_3_Plus_Children_Age_30               == '') ? '0': Individual_3_Plus_Children_Age_30              ), '\\\$','' ) AS Individual_3_Plus_Children_Age_30             
				,	(float) REPLACE ((( Individual_3_Plus_Children_Age_40               == '') ? '0': Individual_3_Plus_Children_Age_40              ), '\\\$','' ) AS Individual_3_Plus_Children_Age_40             
				,	(float) REPLACE ((( Individual_3_Plus_Children_Age_50               == '') ? '0': Individual_3_Plus_Children_Age_50              ), '\\\$','' ) AS Individual_3_Plus_Children_Age_50             
				,	(float) REPLACE ((( Dental_Deductible_Individual                    == '') ? '0': Dental_Deductible_Individual                   ), '\\\$','' ) AS Dental_Deductible_Individual                  
				,	(float) REPLACE ((( Dental_Deductible_Family                        == '') ? '0': Dental_Deductible_Family                       ), '\\\$','' ) AS Dental_Deductible_Family                      
				,	(float) REPLACE ((( Dental_Deductible_Family_Per_Person             == '') ? '0': Dental_Deductible_Family_Per_Person            ), '\\\$','' ) AS Dental_Deductible_Family_Per_Person           
				,	(float) REPLACE ((( Dental_Maximum_Out_Of_Pocket_Individual         == '') ? '0': Dental_Maximum_Out_Of_Pocket_Individual        ), '\\\$','' ) AS Dental_Maximum_Out_Of_Pocket_Individual       
				,	(float) REPLACE ((( Dental_Maximum_Out_Of_Pocket_Family             == '') ? '0': Dental_Maximum_Out_Of_Pocket_Family            ), '\\\$','' ) AS Dental_Maximum_Out_Of_Pocket_Family           
				,	(float) REPLACE ((( Dental_Maximum_Out_Of_Pocket_Family_Per_Person  == '') ? '0': Dental_Maximum_Out_Of_Pocket_Family_Per_Person ), '\\\$','' ) AS Dental_Maximum_Out_Of_Pocket_Family_Per_Person
				,	(float) REGEX_EXTRACT ( (( ( Routine_Dental_Services_Adult == 'Not Covered' ) OR ( Routine_Dental_Services_Adult == '' ) ) ? '0': Routine_Dental_Services_Adult ), '[0-9]+', 0 ) AS Routine_Dental_Services_Adult
				,	(float) REGEX_EXTRACT ( (( ( Basic_Dental_Care_Adult       == 'Not Covered' ) OR ( Basic_Dental_Care_Adult       == '' ) ) ? '0': Basic_Dental_Care_Adult       ), '[0-9]+', 0 ) AS Basic_Dental_Care_Adult
				,	(float) REGEX_EXTRACT ( (( ( Major_Dental_Care_Adult       == 'Not Covered' ) OR ( Major_Dental_Care_Adult       == '' ) ) ? '0': Major_Dental_Care_Adult       ), '[0-9]+', 0 ) AS Major_Dental_Care_Adult
				,	(float) REGEX_EXTRACT ( (( ( Orthodontia_Adult             == 'Not Covered' ) OR ( Orthodontia_Adult             == '' ) ) ? '0': Orthodontia_Adult             ), '[0-9]+', 0 ) AS Orthodontia_Adult
				,	(float) REGEX_EXTRACT ( (( ( Dental_Check_Up_For_Children  == 'Not Covered' ) OR ( Dental_Check_Up_For_Children  == '' ) ) ? '0': Dental_Check_Up_For_Children  ), '[0-9]+', 0 ) AS Dental_Check_Up_For_Children 
				,	(float) REGEX_EXTRACT ( (( ( Basic_Dental_Care_Child       == 'Not Covered' ) OR ( Basic_Dental_Care_Child       == '' ) ) ? '0': Basic_Dental_Care_Child       ), '[0-9]+', 0 ) AS Basic_Dental_Care_Child      
				,	(float) REGEX_EXTRACT ( (( ( Major_Dental_Care_Child       == 'Not Covered' ) OR ( Major_Dental_Care_Child       == '' ) ) ? '0': Major_Dental_Care_Child       ), '[0-9]+', 0 ) AS Major_Dental_Care_Child      
				,	(float) REGEX_EXTRACT ( (( ( Orthodontia_Child             == 'Not Covered' ) OR ( Orthodontia_Child             == '' ) ) ? '0': Orthodontia_Child             ), '[0-9]+', 0 ) AS Orthodontia_Child            
				,	Customer_Service_Phone_Number_Local
				,	Network_URL
				,	Plan_Brochure_URL
				,	Summary_of_Benefits_URL
			;

STORE final_output into 'gs://dataproc-staging-europe-west3-341198046822-nuvlyckm/loaded_dental_plans' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');