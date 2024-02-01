# Flat-Files-Ingestion-With-Azure-Functions

![alt text](https://github.com/datalonewarrior/Flat-Files-Ingestion-With-Azure-Functions/blob/master/Az_Function_Event_Driven_V1.png?raw=true)

# Introduction
This repository contains SQL objects and related visual studio database project resources.
Azure Functions is an event driven, compute-on-demand experience that extends the existing Azure application platform with capabilities to implement code triggered by events occurring in virtually any Azure or 3rd party service as well as on-premises systems. 

# Overview
This solution will copy known flat files format files into azure sql database. The following format delimiters are covered by application can be ammeded to include extra delimiters i:e (',' , '\t' , '\n' , '|' , ';').

# Get started
The following resources needs to be installed and Prerequisites settings.

1.	Visual Studio 2022.
2.	Azure Storage Gen2 Hirerichal Namespace Enabled with a container name "scd".
3. Azure SQL database
4. Azure Function App , Any tier can be used for prod workloads App service plan is recommended.
5. MS Excel with Macro Enabled.
6. Clone the repository: `git clone <Repo URL>`.
7. Navigate to the directory and click on solution file.
8. Publish the function.
9. Locate local.settings.json file on Azure function project in visual studio and update the Azure funtions appsettings on portal.
10. Open the excel and add the headers of required csv file in designated area , Once headers transformed into rows.
11. Move to first tab in the second cell ammended the table name with filename no digits or special characters allowed , provide the business key column details to generate the sql script.
12. Execute the script on database to provide meta data information about file.
13. Upload the file that contain the same table name provided in step 8.
14. A table will be populated with file content , you can change the file content to test SCD 2 scenarios.

# Application Permission
1.	Turn on MSI for azure functions and allow bloab data contributor role.
2.	Configure MSI for azure functions on keyvault.
3.	Locate local.settings.json file on Azure function project and update the Azure funtions appsettings on portal.

# Contents
The directory contain SQL server scripts, application project files and there are sample CSV files can be used for testing.

#  Useful Links 
 1. Azure Functions Getting Started: https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-azure-function
 2. Azure Functions storage binging: https//docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob
 4. host.json metadata file: https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json#functiontimeout
 5. Azure Functions scale and hosting: https://docs.microsoft.com/en-us/azure/azure-functions/functions-scale
 

