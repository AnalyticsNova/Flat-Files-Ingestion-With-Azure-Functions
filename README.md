# Flat-Files-Ingestion-With-Azure-Functions

![alt text](https://github.com/datalonewarrior/Flat-Files-Ingestion-With-Azure-Functions/blob/master/Az_Function_Event_Driven.png?raw=true)

# Introduction
This repository contains SQL objects and related visual studio database project resources.
Azure Functions is an event driven, compute-on-demand experience that extends the existing Azure application platform with capabilities to implement code triggered by events occurring in virtually any Azure or 3rd party service as well as on-premises systems. 

# Overview
This solution will copy known flat files format files into azure sql database. The following format delimiters are covered by application can be ammeded to include extra delimiters i:e (',' , '\t' , '\n' , '|' , ';').

# Get started
The following resources needs to be installed and Prerequisites settings.

1.	Visual Studio 2022.
2.	Azure Storage Gen2 Hirerichal Namespace Enabled with a container name "scd".
3.  Azure SQL database
4.  Azure Function App , Any tier can be used for prod workloads App service plan is recommended.
5.  MS Excel with Macro Enabled.

# Application Permission
1.	Turn on MSI for azure functions and allow bloab data contributor role.
2.	Configure MSI for azure functions on keyvault.
3.	Locate local.settings.json file on Azure function project and update the Azure funtions appsettings on portal.

# Contents
The directory contain SQL server scripts and application project files:


# How to Use
1.	Clone the repository: `git clone <Repo URL>`.
2. Navigate to the directory and click on solution file.
3. File will open in visual studio 2022.

#  Useful Links 
 Azure Functions Getting Started: https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-azure-function
 Azure Functions storage binging: https//docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob
 host. json metadata file: https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json#functiontimeout
 Azure Functions scale and hosting: https://docs.microsoft.com/en-us/azure/azure-functions/functions-scale
 

