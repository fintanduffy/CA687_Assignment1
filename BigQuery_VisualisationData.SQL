SELECT State_Code, ( CASE  
                      WHEN issuer_name LIKE '%Alpha Dental%' THEN 'Alpha Dental'
                      WHEN issuer_name LIKE '%Blue Cross%Blue Shield%' THEN 'Blue Cross Blue Shield'
                      WHEN issuer_name LIKE '%Delta Dental%' THEN 'Delta Dental'
                      WHEN issuer_name LIKE '%DentaQuest%' THEN 'DentaQuest'
                      WHEN issuer_name LIKE '%DentaTrust%' THEN 'DentaTrust'
                      WHEN issuer_name LIKE '%DSM USA Insurance Company%' THEN 'DSM'
                      WHEN issuer_name LIKE '%DentaTrust%' THEN 'DentaTrust'
                      WHEN issuer_name LIKE '%LIBERTY Dental Plan%' THEN 'Liberty'
                      WHEN issuer_name LIKE '%Premier Access%' THEN 'Premier Access'
                      WHEN issuer_name LIKE '%Renaissance%' THEN 'Renaissance'
                      WHEN issuer_name LIKE '%Guardian%' THEN 'Guardian'
                      WHEN UPPER(issuer_name) LIKE '%TRUASSURE%' THEN 'TruAssure'
                      ELSE issuer_name END 
                 ) AS Issuer_Name, MIN( Premium_Child_Age_U14 ) Premium_Child_Age_U14, 
                 MIN(Premium_Adult_Individual_Age_40) Premium_Adult_Individual_Age_40, MIN(Premium_Couple_40) Premium_Couple_40, MIN(Couple_2_Children_Age_40) Couple_2_Children_Age_40, MIN(Individual_2_Children_Age_40) Individual_2_Children_Age_40
FROM `fd-ca687-1.DENTAL_PLANS.ALL_DENTAL_PLANS`
GROUP BY State_Code, Issuer_Name
ORDER BY State_Code, Issuer_Name
