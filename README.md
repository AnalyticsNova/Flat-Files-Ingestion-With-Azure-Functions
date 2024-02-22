# Flat-Files-Ingestion-With-Azure-Functions

![alt text](https://github.com/datalonewarrior/Flat-Files-Ingestion-With-Azure-Functions/blob/master/Az_Function_Event_Driven_V1.png?raw=true)

# Introduction
This repository contains SQL objects and related visual studio database project resources.
Azure Functions is an event driven, compute-on-demand experience that extends the existing Azure application platform with capabilities to implement code triggered by events occurring in virtually any Azure or 3rd party service as well as on-premises systems. 

# Overview
This solution will copy known flat files format files into azure sql database. The following format delimiters are covered by application can be ammeded to include extra delimiters i:e (',' , '\t' , '\n' , '|' , ';').

# Get started
The following resources needs to be installed and Prerequisites settings.

## Flat File Format
1.  please make sure flat file contain only above delimiters and it should be of correct format. Avoid using SQL keywords as column name.
2.  No spaces between the column names.
3.  You can use notepad++ to check the file content.
4.  A file with similar name can only be processed once , add unique identifier to every file upload i:e datetime.
5.  File name should contain unique identifiers for every upload , duplicates file names will be excluded from processing.

## Resources

1.	Visual Studio 2022.
2.	SQL Server Management Studio or Data Studio.
3.	Azure Storage Gen2 Hirerichal Namespace Enabled with a container name "scd".
4. Azure SQL database.
5. Azure Function App , Any tier can be used for prod workloads App service plan is recommended.
6. MS Excel with Macro Enabled.

## Initial Setup
8. Clone the repository: `git clone <Repo URL>`.
9. Navigate to the directory and click on solution file.
10. Publish the function.
11. Locate local.settings.json file on Azure function project in visual studio and update the Azure funtions appsettings on portal.
12. Turn on MSI for azure functions and allow bloab data contributor role.
13. Configure MSI for azure functions on keyvault.
14. Open the excel and add the headers of required csv file in designated area , Once headers transformed into rows.
15. Move to first tab in the second cell ammended the table name with filename no digits or special characters allowed.
16. provide the business key column details to generate the sql script, composite key columns are not supported require minor changes in sql procedures.
17. Execute the script on database to provide meta data information about file.
18. Upload the file that contain the same table name provided in step 8.
19. A table will be populated with file content , you can change the file content to test SCD 2 scenarios.

# Contents
The directory contain SQL server scripts, application project files and there are sample CSV files can be used for testing.

#  Useful Links 
 1. Azure Functions Getting Started: [Click Here](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-azure-function)
 2. Azure Functions storage binging: [Click Here](https//docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob)
 4. host.json metadata file: [Click Here](https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json#functiontimeout)
 5. Azure Functions scale and hosting: [Click Here](https://docs.microsoft.com/en-us/azure/azure-functions/functions-scale)
 

