
/****** Object:  Schema [Audit]    Script Date: 29/01/2024 10:22:24 ******/
CREATE SCHEMA [Audit]
GO
/****** Object:  Schema [Fileloader]    Script Date: 29/01/2024 10:22:24 ******/
CREATE SCHEMA [Fileloader]
GO
/****** Object:  Schema [Reference]    Script Date: 29/01/2024 10:22:24 ******/
CREATE SCHEMA [Reference]
GO
/****** Object:  Schema [staging]    Script Date: 29/01/2024 10:22:24 ******/
CREATE SCHEMA [staging]
GO
/****** Object:  Schema [Utility]    Script Date: 29/01/2024 10:22:24 ******/
CREATE SCHEMA [Utility]
GO
/****** Object:  Sequence [Fileloader].[Seq_No]    Script Date: 29/01/2024 10:22:24 ******/
CREATE SEQUENCE [Fileloader].[Seq_No] 
 AS [bigint]
 START WITH 1
 INCREMENT BY 1
 MINVALUE -9223372036854775808
 MAXVALUE 9223372036854775807
 CACHE 
GO


/****** Object:  Table [Fileloader].[Files_To_Load]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Fileloader].[Files_To_Load](
	[Sr_No] [int] IDENTITY(1,1) NOT NULL,
	[Seq_No] [bigint] NOT NULL,
	[Schema_Name] [varchar](8000) NULL,
	[Table_Name] [varchar](8000) NULL,
	[File_Delimiter] [nvarchar](4000) NULL,
	[Table_Definition] [varchar](8000) NULL,
	[File_ID] [nvarchar](4000) NULL,
	[File_Name] [varchar](8000) NULL,
	[File_Name_No_Extension] [varchar](8000) NULL,
	[File_Date_Time] [varchar](8000) NULL,
	[File_Source_Folder] [varchar](8000) NULL,
	[File_Archive_Folder] [varchar](8000) NULL,
	[Validation_Flg] [varchar](8000) NULL,
	[Load_Flg] [int] NULL,
	[File_Row_Count] [int] NULL,
	[Records_Inserted] [int] NULL,
	[Records_Updated] [int] NULL,
	[Records_Deleted] [int] NULL,
	[Records_Deleted_Staging] [int] NULL,
	[Records_Duplicated] [int] NULL,
	[Versions_Updated] [int] NULL,
	[Start_Date_Time] [datetime] NULL,
	[End_Date_Time] [datetime] NULL,
	[Error_Message] [varchar](8000) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [Fileloader].[Files_To_Load_History]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Fileloader].[Files_To_Load_History](
	[Sr_No] [int] NOT NULL,
	[Seq_No] [bigint] NOT NULL,
	[Schema_Name] [varchar](8000) NULL,
	[Table_Name] [varchar](8000) NULL,
	[File_Delimiter] [nvarchar](4000) NULL,
	[Table_Definition] [varchar](8000) NULL,
	[File_ID] [nvarchar](4000) NULL,
	[File_Name] [varchar](8000) NULL,
	[File_Name_No_Extension] [varchar](8000) NULL,
	[File_Date_Time] [varchar](8000) NULL,
	[File_Source_Folder] [varchar](8000) NULL,
	[File_Archive_Folder] [varchar](8000) NULL,
	[Validation_Flg] [varchar](8000) NULL,
	[Load_Flg] [int] NULL,
	[File_Row_Count] [int] NULL,
	[Records_Inserted] [int] NULL,
	[Records_Updated] [int] NULL,
	[Records_Deleted] [int] NULL,
	[Records_Deleted_Staging] [int] NULL,
	[Records_Duplicated] [int] NULL,
	[Versions_Updated] [int] NULL,
	[Start_Date_Time] [datetime] NULL,
	[End_Date_Time] [datetime] NULL,
	[Error_Message] [varchar](8000) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [Fileloader].[Table_Structure]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Fileloader].[Table_Structure](
	[Table_ID] [int] NULL,
	[Table_Name] [varchar](4000) NULL,
	[Field_Name] [varchar](4000) NULL,
	[Field_Type] [varchar](4000) NULL,
	[Field_Sort] [int] NULL,
	[Valid_From] [datetime] NULL,
	[Valid_To] [datetime] NULL,
	[Version] [int] NULL,
	[Target_Data_Type] [varchar](8000) NULL,
	[Field_Flg] [varchar](8000) NULL,
	[Schema_Name] [varchar](500) NULL,
	[Is_Active] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [Fileloader].[Files_To_Load] ADD  DEFAULT (abs(checksum(newid()))) FOR [Seq_No]
GO
ALTER TABLE [Fileloader].[Files_To_Load] ADD  CONSTRAINT [dt_Error_Message]  DEFAULT (NULL) FOR [Error_Message]
GO
/****** Object:  StoredProcedure [dbo].[sp_dim_date]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [dbo].[sp_dim_date]
as

drop table if exists dbo.DIM_Date 

DECLARE @StartDate  date = '19000101';

DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(YEAR, 300, @StartDate));

;WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
),
d(d) AS 
(
  SELECT DATEADD(DAY, n, @StartDate) FROM seq
),
src AS
(
  SELECT
    TheDate         = CONVERT(date, d),
    TheDay          = DATEPART(DAY,       d),
    TheDayName      = DATENAME(WEEKDAY,   d),
    TheWeek         = DATEPART(WEEK,      d),
    TheISOWeek      = DATEPART(ISO_WEEK,  d),
    TheDayOfWeek    = DATEPART(WEEKDAY,   d),
    TheMonth        = DATEPART(MONTH,     d),
    TheMonthName    = DATENAME(MONTH,     d),
    TheQuarter      = DATEPART(Quarter,   d),
    TheYear         = DATEPART(YEAR,      d),
    TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
    TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
    TheDayOfYear    = DATEPART(DAYOFYEAR, d)
  FROM d
),
dim AS
(
  SELECT
    TheDate, 
    TheDay,
    TheDaySuffix        = CONVERT(char(2), CASE WHEN TheDay / 10 = 1 THEN 'th' ELSE 
                            CASE RIGHT(TheDay, 1) WHEN '1' THEN 'st' WHEN '2' THEN 'nd' 
                            WHEN '3' THEN 'rd' ELSE 'th' END END),
    TheDayName,
    TheDayOfWeek,
    TheDayOfWeekInMonth = CONVERT(tinyint, ROW_NUMBER() OVER 
                            (PARTITION BY TheFirstOfMonth, TheDayOfWeek ORDER BY TheDate)),
    TheDayOfYear,
    IsWeekend           = CASE WHEN TheDayOfWeek IN (CASE @@DATEFIRST WHEN 1 THEN 6 WHEN 7 THEN 1 END,7) 
                            THEN 1 ELSE 0 END,
    TheWeek,
    TheISOweek,
    TheFirstOfWeek      = DATEADD(DAY, 1 - TheDayOfWeek, TheDate),
    TheLastOfWeek       = DATEADD(DAY, 6, DATEADD(DAY, 1 - TheDayOfWeek, TheDate)),
    TheWeekOfMonth      = CONVERT(tinyint, DENSE_RANK() OVER 
                            (PARTITION BY TheYear, TheMonth ORDER BY TheWeek)),
    TheMonth,
    TheMonthName,
    TheFirstOfMonth,
    TheLastOfMonth      = MAX(TheDate) OVER (PARTITION BY TheYear, TheMonth),
    TheFirstOfNextMonth = DATEADD(MONTH, 1, TheFirstOfMonth),
    TheLastOfNextMonth  = DATEADD(DAY, -1, DATEADD(MONTH, 2, TheFirstOfMonth)),
    TheQuarter,
    TheFirstOfQuarter   = MIN(TheDate) OVER (PARTITION BY TheYear, TheQuarter),
    TheLastOfQuarter    = MAX(TheDate) OVER (PARTITION BY TheYear, TheQuarter),
    TheYear,
    TheISOYear          = TheYear - CASE WHEN TheMonth = 1 AND TheISOWeek > 51 THEN 1 
                            WHEN TheMonth = 12 AND TheISOWeek = 1  THEN -1 ELSE 0 END,      
    TheFirstOfYear      = DATEFROMPARTS(TheYear, 1,  1),
    TheLastOfYear,
    IsLeapYear          = CONVERT(bit, CASE WHEN (TheYear % 400 = 0) 
                            OR (TheYear % 4 = 0 AND TheYear % 100 <> 0) 
                            THEN 1 ELSE 0 END),
    Has53Weeks          = CASE WHEN DATEPART(WEEK,     TheLastOfYear) = 53 THEN 1 ELSE 0 END,
    Has53ISOWeeks       = CASE WHEN DATEPART(ISO_WEEK, TheLastOfYear) = 53 THEN 1 ELSE 0 END,
    MMYYYY              = CONVERT(char(2), CONVERT(char(8), TheDate, 101))
                          + CONVERT(char(4), TheYear),
    Style101            = CONVERT(char(10), TheDate, 101),
    Style103            = CONVERT(char(10), TheDate, 103),
    Style112            = CONVERT(char(8),  TheDate, 112),
    Style120            = CONVERT(char(10), TheDate, 120)
  FROM src
)
--SELECT * FROM dim
--  ORDER BY TheDate
--    OPTION (MAXRECURSION 0);



  SELECT * INTO dbo.DIM_Date FROM dim

  OPTION (MAXRECURSION 0);
 -- drop table dbo.DIM_Date 
GO
/****** Object:  StoredProcedure [Fileloader].[Load_File_To_Load_History]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE proc [Fileloader].[Load_File_To_Load_History]
as

Insert into [Fileloader].[Files_To_Load_History] 
select distinct * from [Fileloader].[Files_To_Load]
except
select distinct * from [Fileloader].[Files_To_Load_History]

---- deleting non processed file records
--Delete from [Fileloader].[Files_To_Load_History] where Validation_Flg = 'Success' and Load_Flg = 1 and File_Archive_Folder is null 
Delete from [Fileloader].[Files_To_Load_History] where Validation_Flg = 'Success' and Load_Flg is null and [Error_Message] is null
GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Add_Missing_Columns_With_DataType]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE Proc [Fileloader].[Sp_Add_Missing_Columns_With_DataType]							
 								
@SourceSchema Nvarchar(max) ,							
@SourceObject Nvarchar(max) ,
@TargetSchema Nvarchar(max) ,									
@TargetObject Nvarchar(max) 							
as 								
Begin								
								
IF 1 = 1						
								
Begin								
--declare @SourceSchema Nvarchar(max) = 'Staging_Horizon'								
--declare @SourceObject Nvarchar(max) = 'V_abc'
--declare @TargetSchema Nvarchar(max) = 'Staging_Horizon'									
--declare @TargetObject Nvarchar(max) = 'abc'	
declare @TargetObjectwithTargetSchema Nvarchar(max) = @TargetSchema+'.'+@TargetObject	
--print 	@TargetObjectwithTargetSchema				
Declare  @COLUMN_NAME nvarchar(max),@DATA_TYPE nvarchar(max),@CHARACTER_MAXIMUM_LENGTH nvarchar(max),@Sql nvarchar(max)  = ''								
								
declare  ColumnCheck Cursor  								
								
---------------------------------------------Enter Source Table Information-----------------------------------------------------------------								
for  ( select s.COLUMN_NAME,s.DATA_TYPE,case when  cast(s.CHARACTER_MAXIMUM_LENGTH as nvarchar(max)) =  CAST(-1 as nvarchar(max))  then 'Max'								
                                                     else cast(s.CHARACTER_MAXIMUM_LENGTH as nvarchar(max))								
                                                        end as CHARACTER_MAXIMUM_LENGTH 								
         from INFORMATION_SCHEMA.COLUMNS s where Table_Name =  @SourceObject AND TABLE_SCHEMA = @SourceSchema ) 								
--------------------------------------------------------------------------------------------------------------------------------------------         								
         								
open ColumnCheck								
								
Fetch Next from ColumnCheck into @COLUMN_NAME,@DATA_TYPE,@CHARACTER_MAXIMUM_LENGTH								
								
while  @@FETCH_STATUS = 0								
								
begin  								
								
								
/*-----------------------------------------Non Exist Columns-------------------------------------------------------------------------------------*/								
								
/*----Not Null Column Lenght data types */								
 if @CHARACTER_MAXIMUM_LENGTH is not null 								
 								
   Begin								
  								
         Begin try								
                      if not exists 								
                       (select 1 from INFORMATION_SCHEMA.COLUMNS T   								
                         where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema )								
                          								
						 Begin		
						   Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' add '+quotename(@COLUMN_NAME)+' '+@DATA_TYPE+'('+@CHARACTER_MAXIMUM_LENGTH+')'		
		  						
							       --Print (@Sql)	
							       Exec (@Sql)     	
						  End		
                    								
          END TRY								
             								
		  Begin catch						
		  SELECT ERROR_MESSAGE() AS 'Message' 						
		  end catch                          						
                                    								
    End								
 								
 								
 								
/*---- Null Column Lenght data types*/ 								
 if @CHARACTER_MAXIMUM_LENGTH is null 								
 								
   Begin								
  								
       begin try								
                      if not exists 								
                       (select 1 from INFORMATION_SCHEMA.COLUMNS T   								
                         where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema)								
                          								
						 Begin		
						   Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' add '+quotename(@COLUMN_NAME)+' '+@DATA_TYPE+''		
		  						
							       --Print (@Sql)	
							       Exec (@Sql)     	
						  End		
                    								
           END TRY								
             								
		  begin catch						
		  SELECT ERROR_MESSAGE() AS 'Message' 						
		  end catch                             						
                                    								
    End								
               								
 								
--/*-------------------------------------Existing Columns with different Data Types--------------------------------------------------------------*/ 								
                                								
--/*----Not Null Column Lenght data types */                        								
-- if @CHARACTER_MAXIMUM_LENGTH is not null 								
 								
--   Begin								
								
								
--         begin try								
--                      if exists 								
--                       (select 1 from INFORMATION_SCHEMA.COLUMNS T   								
--                         where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema )								
                          								
--						 Begin		
--						   Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' alter  Column  '+quotename(@COLUMN_NAME)+'  '+@DATA_TYPE+'('+@CHARACTER_MAXIMUM_LENGTH+')'		
		  						
--							      --Print (@Sql)	
--							      Exec (@Sql)     	
--						  End		
                    								
--           END TRY								
             								
--		  begin catch						
--		  SELECT ERROR_MESSAGE() AS 'Message' 						
--		  end catch                          						
                                    								
--    End  								
 								
 								
 								
-- -- Null Column Lenght data types 								
-- if @CHARACTER_MAXIMUM_LENGTH is null 								
 								
--	   Begin							
	  							
--		   begin try						
--						  if exists 		
--						   (select 1 from INFORMATION_SCHEMA.COLUMNS T   		
--							 where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema)	
	                          							
--							 Begin	
--								Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' alter  Column  '+quotename(@COLUMN_NAME)+'  '+@DATA_TYPE+''
			  					
			  					
--								      --Print (@Sql)
--								      Exec (@Sql)     
--							  End	
	                    							
--			   END TRY					
	             							
--			  begin catch					
--			  SELECT ERROR_MESSAGE() AS 'Message' 					
--			  end catch                             					
	                                    							
--		End						
	
	
	           							
								
Fetch Next from ColumnCheck into   @COLUMN_NAME,@DATA_TYPE,@CHARACTER_MAXIMUM_LENGTH								
end 								
								
								
close ColumnCheck 								
								
deallocate ColumnCheck 								
  								
								
End								
								
								
else								
								
SELECT ERROR_MESSAGE() AS 'Message'								
								
						
  								
  								
End
GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Create_Production_Table]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE PROC [Fileloader].[Sp_Create_Production_Table]
   @tablename nvarchar(max),
   @column_list_with_StagingColumnsDataType nvarchar(max)
  ,@staging_schema nvarchar(max)
  ,@prod_schema nvarchar(max)
  ---,@tabledefinition nvarchar(max)  

 as
 begin

 begin try
 begin tran

 SET DATEFORMAT dmy;

 --declare @tablename nvarchar(max) = 'abc'
 --declare @staging_schema nvarchar(max) = 'Staging_Horizon'
 --declare @tabledefinition nvarchar(max) = '[Id]  Nvarchar(4000),[Name]  Nvarchar(4000),[Gender]  Nvarchar(4000)'


 declare @Debug int  = 0
 --declare @prodschema nvarchar(max) = replace(@staging_schema,'Staging_','')
 declare @viewname nvarchar(max) = 'V_'+@tablename
 -- double check logic in select statement '['+upper(left(@tablename,4)+'_'+'REF')+']'
 --declare @seq_no varchar(max) = Cast((NEXT VALUE FOR  [Fileloader].[Seq_No]) as Nvarchar(max))+'_'+'Null';
 --print @seq_no
 declare @businesskey nvarchar(max) 
  -- set @businesskey = (select top 1 quotename(Field_Name)  from  [Fileloader].[Table_Structure]  where  Table_Name = ''+@tablename+'' and [Schema_Name] = ''+@prodschema+''and Field_Flg = 'Business_Key' and Is_Active = 1 )
 declare @surrogatekey nvarchar(max) = '['+'Sk'+'_'+'Id'+']'
 declare @scdcolumn nvarchar(max) = '[Hash] BINARY(20)
                                      ,[Latest_Indicator] BIT NOT NULL Default (1)
									 ,[Valid_From] DATETIME2(0) NOT NULL DEFAULT (GETDATE())
									 ,[Valid_Until] DATETIME2(0) NOT NULL DEFAULT ''31-DEC-9999 23:59:59'',
									  [Created] DATETIME2(0) NOT NULL DEFAULT GETDATE()
									 ,[Created_By] NVARCHAR(200) NOT NULL Default (suser_sname())
									 ,[Last_Updated] DATETIME2(0) DEFAULT (GETDATE())
									 ,[Last_Updated_By] NVARCHAR(200) NOT NULL Default (suser_sname())
									 ,File_Seq_No Nvarchar(Max) Null'
 --print @scdcolumn
 
 --==========Target Columns =============================================================
  declare @targetcolumns nvarchar(max) = ''
 declare @sql nvarchar(max) ='			
set @columnName1  = STUFF ((select '','' + quotename(Field_Name)+'' ''+Target_Data_Type 
                    from  [Fileloader].[Table_Structure]  	
                     where 	Table_Name = '''+@tablename+''' and Schema_Name = '''+@prod_schema+'''
					 and Is_Active = 1
					   Order By Field_Sort
                         FOR XML PATH('''')), 1, 1,'''')   
                           '
if @debug = 1 print @sql
 else 
--exec sp_executesql @sql ,N'@columnName1 nvarchar(max) output' , @columnName1 = @targetcolumns output	
--print @targetcolumns
-- if you like to use generic nvarchar(4000) use below statement and hide above.
set @targetcolumns = @column_list_with_StagingColumnsDataType
---====================================================================================
declare @combinedcolumns nvarchar(max) = @surrogatekey+'Int Identity(1,1)'+','+@targetcolumns+','+@scdcolumn
--print @combinedcolumns 

--=====Create Prod Table ===============================================================
 declare @Sql3 nvarchar(max)= '
 if not exists ( select 1 from sys.tables where name ='''+@tablename+''' and  schema_name(schema_id) = '''+@prod_schema+''' )
  begin
		  CREATE TABLE ['+@prod_schema+'].['+@tablename+']
		 
		(   
			  '+@combinedcolumns+'  ,
			  INDEX ix_Hash_'+@tablename+'  NONCLUSTERED ([Hash])  ,
			  CONSTRAINT PK__'+@tablename+'__Id PRIMARY KEY('+@surrogatekey+'),
           -- CONSTRAINT FK__'+@tablename+'__Purge_Status_Id FOREIGN KEY (Purge_Status_Id) REFERENCES Utility.Purge_Status(Id),
           -- CONSTRAINT UK__'+@tablename+'__FBI_Latest_Indicator UNIQUE(FBI_Latest_Indicator)

		)

   end
 '
if @Debug = 1 
Print @sql3 
else 
exec sp_executesql @sql3


commit tran
end try


begin catch
if @@TRANCOUNT > 0
 rollback tran
 --exec audit.Log_Action 'Horizon File Loader Error: Check [Fileloader].[Files_To_Load_History]',4,'HFL'
 end catch


end
GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Create_Production_View]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [Fileloader].[Sp_Create_Production_View] @Prod_schema [nvarchar](max),@tablename [nvarchar](max) AS

SET DATEFORMAT dmy;

--=============Test Parameters===================================
--declare @schemaname nvarchar(max) = 'Staging' 
--declare @tablename  nvarchar(max) = 'abc' 

declare @debug int = 0
declare @viewname  nvarchar(max) = 'V_'+@tablename
declare @Prodschema nvarchar(max) = @Prod_schema
declare @tablename_with_schema nvarchar(max) = @Prodschema +'.'+@tablename
declare @viewname_with_schema nvarchar(max) = @Prodschema +'.'+@viewname
 
--=======Create View=====================================================================

if not exists (select * from sys.views where name = ''+@viewname+''  and schema_name(Schema_Id) = ''+@Prodschema+'' )
exec ('CREATE VIEW '+@viewname_with_schema+'  AS  select * from '+@tablename_with_schema+' where Latest_Indicator = 1 ')

else

exec ('Alter VIEW '+@viewname_with_schema+'  AS  select * from '+@tablename_with_schema+' where Latest_Indicator = 1 ')
GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Create_Staging_View]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






Create PROC [Fileloader].[Sp_Create_Staging_View] 
@file_header [nvarchar](max),
@column_list_with_StagingColumnsDataType [nvarchar](max)
,@staging_schema [nvarchar](max),
@prod_schema [nvarchar](max),@tablename [nvarchar](max) AS
begin


begin try 
begin tran

SET DATEFORMAT dmy;

----=============Test Parameters===================================
--declare @staging_schema nvarchar(max) = 'Staging' 
--declare @tablename  nvarchar(max) = 'people' 
--declare @column_list_with_StagingColumnsDataType nvarchar(max) = N'[Indexx] Nvarchar(4000),[User Id] Nvarchar(4000),[First Name] Nvarchar(4000),[Last Name] Nvarchar(4000),[Sex] Nvarchar(4000),[Email] Nvarchar(4000),[Phone] Nvarchar(4000),[Date of birth] Nvarchar(4000),[Job Title] Nvarchar(4000)'
--declare @productionschema nvarchar(max) =  'dbo'
--declare @prod_schema nvarchar(max) =  ''
--declare @file_header nvarchar(max) = N'[Indexx],[User Id],[First Name],[Last Name],[Sex],[Email],[Phone],[Date of birth],[Job Title]'
--print(@file_header)

declare @debug int = 0
declare @viewname  nvarchar(max) =  'V_'+@tablename
declare @viewname_with_schema nvarchar(max) = @staging_schema +'.'+@viewname
declare @tablename_with_schema nvarchar(max) = @staging_schema +'.'+@tablename
declare @productionschema nvarchar(max) =  @prod_schema

 --==========Target Columns ===========================================================
 declare @targetcolumns nvarchar(max) = ''
 declare @sql nvarchar(max) ='			
set @columnName1  = STUFF ((select   '','' + Replace(quotename(Field_Name),''['',''Cast(['')+'' AS ''+Target_Data_Type +'') AS ''  + quotename(Field_Name) 
                    from  [Fileloader].[Table_Structure]  	
                     where 	Table_Name = '''+@tablename+''' and Schema_Name = '''+@productionschema+'''
					 and Is_Active = 1
					  Order By Field_Sort
                         FOR XML PATH('''')), 1, 1,'''')   
                           '
if @debug = 1 
	print @sql
 else 
	exec sp_executesql @sql ,N'@columnName1 nvarchar(max) output' , @columnName1 = @targetcolumns output	
--print @targetcolumns
-- if you like to use generic nvarchar(4000) use below statement and hide above.
set @targetcolumns = @file_header
--print(@targetcolumns)
--===========Hash Column================================================================
declare @HasCol nvarchar(max) = REPLACE(REPLACE(Replace(replace(@file_header,'Nvarchar(4000)',''),',','+'),'[','ISNULL(['),']','],''NA'')')
--print @HasCol

--=======Drop View=====================================================================
if exists (select * from sys.views where name = ''+@viewname+''  and schema_name(Schema_Id) = ''+@staging_schema+'' )
exec ('Drop VIEW '+@viewname_with_schema+'')

--=======Create View=====================================================================
if not exists (select * from sys.views where name = ''+@viewname+''  and schema_name(Schema_Id) = ''+@staging_schema+'' )
--exec ('CREATE VIEW '+@viewname_with_schema+'  AS  select '+@targetcolumns+', HASHBYTES(''SHA1'','+@HasCol+') as Hash from '+@tablename_with_schema+'')
declare @sql1 nvarchar(max) ='CREATE VIEW '+@viewname_with_schema+'  AS  select '+@file_header+', HASHBYTES(''SHA1'','+@HasCol+') as Hash from '+@tablename_with_schema+''
if @debug = 1 
	print @sql1
 else
	exec sp_executesql @sql1



commit tran
end try

begin catch
if @@TRANCOUNT > 0
 rollback tran
 
 end catch



 end



GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Files_To_Load]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROC [Fileloader].[Sp_Files_To_Load] @Fileid [Nvarchar](4000),@Filename [varchar](1000),@Tablename [varchar](1000),@Filenamenoextension [varchar](1000),@tabledefinition [nvarchar](max),@Validationflg [varchar](1000),@filesource [varchar](4000),@filerowcount [int],@errormessage [varchar](8000) , @Sr_No int output ,@SchemaName varchar(8000), @filedatetime varchar(8000), @filedelimiter nvarchar(80) AS 

Insert into Fileloader.Files_To_Load (
 [File_ID]
,[File_Name] 
,[Table_Name] 
,[File_Name_No_Extension] 
,[Table_Definition]
,[Validation_Flg]
,[File_Source_Folder]
,[Start_Date_Time]
,[File_Row_Count]
,[Error_Message]
,[Schema_Name]
,[File_Date_Time]
,[File_Delimiter]
  )
select 
 @Fileid
,@Filename
,@Tablename
,@Filenamenoextension
,@tabledefinition
,@Validationflg
,@filesource
,Null
,@filerowcount
,@errormessage
,@SchemaName
,@filedatetime
,@filedelimiter
;

set @Sr_No = SCOPE_IDENTITY()
return
GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Get_Fileloader_Parameter]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [Fileloader].[Sp_Get_Fileloader_Parameter]
as 
-- change it
select 1

--select  [adlsAccountFQDN],[applicationId],[ArchiveFolder],[clientSecret],[FileDelimiter],[SchemaName],[SourceFolder],[sqlconnectionstring],[tenantId],[FileWordToRemove]
 
--from 
--(

--SELECT  [Parameter]      ,[Parameter_Value]      FROM [Utility].[Configuration]  
--where Module_Id=5   and Parameter 
--in ('adlsAccountFQDN','applicationId','ArchiveFolder','clientSecret','FileDelimiter','SchemaName','SourceFolder','sqlconnectionstring','tenantId','FileWordToRemove'   )    and [Active_Indicator]=1  

--) a 

--pivot 

--(
-- min([Parameter_Value]  )
--for [Parameter]
--in ([adlsAccountFQDN],[applicationId],[ArchiveFolder],[clientSecret],[FileDelimiter],[SchemaName],[SourceFolder],[sqlconnectionstring],[tenantId],[FileWordToRemove])

--)
--as piv
GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Get_Files_To_Load_Information]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create   proc [Fileloader].[Sp_Get_Files_To_Load_Information]
@datatablename [nvarchar](max) 
as 
begin 

select a.Seq_No,a.Table_Name, a.[File_Name], a.File_Name_No_Extension, a.File_Source_Folder, a.Table_Definition, a.Sr_No ,a.File_Delimiter ,a.File_ID  from [Fileloader].[Files_To_Load] a 
--Inner Join [Fileloader].[Table_Structure] b on Replace(a.Table_Name,' ','') = Replace(b.Table_Name,' ','') and b.Field_Flg = 'Business_Key' and b.Is_Active = 1
where a.[File_Name]+a.[File_ID]= ''+@datatablename+'' 
end
GO
/****** Object:  StoredProcedure [Fileloader].[Sp_Insert_Into_Production_Table]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



Create PROC [Fileloader].[Sp_Insert_Into_Production_Table]
 @tablename nvarchar(max),
 @staging_schema nvarchar(max),
 @prod_schema nvarchar(max),
 @Sr_No int,
 @filenamenoextension nvarchar(max) ,
 @fileseqno nvarchar(max), 
 @FileRowCount int,
 @fileheader nvarchar(max)
--, @debug int = 0

 as 

 Begin Try 
 Begin Tran

SET DATEFORMAT dmy;
--====================Test Parameters================	
--declare @tablename nvarchar(max) = 'people'
--declare @schemaname nvarchar(max) = 'Staging'	
--declare @ArchiveFolder nvarchar(max) ='/test'
--declare @Sr_No int = 2 	
--declare @filenamenoextension nvarchar(max) = 'abc.csv'
--declare @fileseqno nvarchar(max) = 'abc.csv'
--declare @prod_schema nvarchar(max) = 'dbo'
--declare @FileRowCount varchar(max) = '1'
--declare @fileheader nvarchar(max) = '[Indexx],[User Id],[First Name],[Last Name],[Sex],[Email],[Phone],[Date of birth],[Job Title]'
--declare @staging_schema nvarchar(max) = 'staging'

declare @debug int = 0			
declare @viewname nvarchar(max) = 'V_'+@tablename
declare @prodschema nvarchar(max) = @prod_schema
declare @businesskey nvarchar(max) 
set @businesskey = (select STRING_AGG(quotename(ISNULL(Field_Name, ' ')),'+') As Business_Key from  [Fileloader].[Table_Structure]  where  Table_Name = ''+@tablename+'' and [Schema_Name] = ''+@prodschema+''and Field_Flg = 'Business_Key' and Is_Active = 1 )
declare @businessKeyMissingError nvarchar(max) = IIF( @businesskey is null ,'Business Key Not Availible,','')
print @businesskey
declare @Records_Updated varchar(max)
declare @Records_Inserted varchar(max)
declare @Records_Deleted varchar(max) 
declare @Records_Deleted_Staging varchar(max) 
declare @Records_Duplicated varchar(max)
declare @Load_Date DATETIME2(0) = GETDATE()
declare @valid_from datetime2 = GETDATE()
declare @deleteemptyrows varchar(max) ='0'
declare @updateinprod varchar(max)='0'
declare @deleteinstag varchar(max)='0'
declare @deletedups varchar(max)='0'
declare @deleteghostrecordsstag varchar(max) ='0'
declare @updateoldrecords varchar(max)='0' 
declare @insertnewrecords varchar(max) ='0'
declare @updateinprod2 varchar(max)='0'
declare @insertupdatedrecords varchar(max) = '0'
declare @Versions_Updated varchar(max) = '0'
declare @Sr_No1 varchar(max)  = Cast(@Sr_No as varchar(max))
declare @FileRowCount1 varchar(max) = Cast(@FileRowCount as varchar(max))



---==================Updating Files_To_Load Table with load start date time-----
 Declare @sql9 nvarchar(max) =' 
 Update [Fileloader].[Files_To_Load] Set Start_Date_Time = GETDATE() ,File_Row_Count = cast('+@FileRowCount1+' as int),Table_Definition = '''+@fileheader+'''  Where Sr_No =  cast('+@Sr_No1+' as int) and Validation_Flg = ''Success''
 '
if @debug = 1 Print @sql9 
else 
exec sp_executesql @sql9

---==================Delete empty rows from staging table---------------------------
DECLARE @NewLineChar AS CHAR(2) = CHAR(13) + CHAR(10)
declare @v_sql nvarchar(max) = '';
select @v_sql = 'DELETE FROM '+@staging_schema+'.'+@tablename+' WHERE '
--print @v_sql
select @v_sql = @v_sql + data.name + ' IS NULL ' + @NewLineChar + ' AND '
from
(select TOP(10000000) quotename(c.name) as [name]
from   sys.schemas s 
join   sys.objects o on s.schema_id = o.schema_id 
join   sys.columns c on o.object_id = c.object_id
where s.name = ''+@staging_schema+'' and o.name = ''+@tablename+''
order by column_id
) data
select @v_sql = SUBSTRING(cast(@v_sql AS NVARCHAR(max)), 1, LEN(@v_sql) - 4)
 if @debug = 1 print @v_sql
 else 
exec sp_executesql @v_sql
                   
				       set @deleteemptyrows = @@ROWCOUNT

---=================Target columns Extracted from View----------------------------------
declare @targetcolumns nvarchar(max) = ''	
declare @sql nvarchar(max) ='			
set @columnName1  = STUFF ((select '','' + quotename(COLUMN_NAME) from INFORMATION_SCHEMA.COLUMNS 	
                     where 	Table_Name = '''+@viewname+''' and TABLE_SCHEMA = '''+@staging_schema+'''
                         FOR XML PATH('''')), 1, 1,'''')   
                           '
if @debug = 1 print @sql
 else 
exec sp_executesql @sql ,N'@columnName1 nvarchar(max) output' , @columnName1 = @targetcolumns output	

declare @sourcecolumns nvarchar(max) = replace(@targetcolumns,'[','s.[')
-- print @targetcolumns									
						
	              
					  								 											
--==================Deleted records will be cleared from staging and prod table latest indicator will be 0 

if exists (select * from INFORMATION_SCHEMA.Columns where Table_Schema = @staging_schema and TABLE_NAME = @tablename and COLUMN_NAME = 'QCID')	
Begin		
              ----=========generating columns to check for deletes and empty rows
declare @col nvarchar(max)= ''				
select @col +=  ('s.'+quotename(s.COLUMN_NAME) +' Is Null And ' +char(13)) from ( select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = @staging_schema and Table_Name = @tablename and COLUMN_NAME <> 'QCID') as s				
declare @columnname nvarchar(max) = ''				
set @columnname = LEFT(@col,len(@col)-5)
declare @updatedeletedrecords nvarchar(max)
--print @columnname	

 
End

--================delete duplicate from staging and prod
declare @step0 nvarchar(max) = 
'
    Delete FROM '+@staging_Schema+'.'+@viewname+' WHERE '+@businesskey+' IN 
    (
    SELECT  S.'+@businesskey+' FROM '+@prodschema+'.'+@tablename+'  as T	 
	 Join  '+@staging_Schema+'.'+@viewname+' as S on T.'+@businesskey+' = S.'+@businesskey+'  AND S.Hash = T.Hash
     where T.Latest_Indicator = 1
    )
'
if @debug = 1 print @step0
 else 
exec sp_executesql @step0 
							 set @deletedups = @@rowcount 
                                               

								-----Note:The below Sqls Used for SCD-2 in prod table --------------------------

--================Step 1 Update Latest_Indicator = 0 for old records 	
declare @step1 nvarchar(max) = 
'
	 Update  T	set T.Latest_Indicator = 0 ,T.Valid_Until = ''' + convert(varchar(25),DATEADD(ss,-1,@Load_Date),121) + '''
	 from  '+@prodschema+'.'+@tablename+'  as T	 
	 Join  '+@staging_schema+'.'+@viewname+' as S	 on T.'+@businesskey+' = S.'+@businesskey+' 
     where T.Latest_Indicator = 1 ;
'
if @debug = 1 print @step1
 else 
exec sp_executesql @step1 
							 set @Versions_Updated = @@rowcount 
							 							                 
--================Step 2 Update Existing Records   
declare @step2 nvarchar(max) =  
'
	 Insert Into '+@prodschema+'.'+@tablename+' ('+@targetcolumns+',File_Seq_No)   	
	 select '+@targetcolumns+','''+@fileseqno+'''
	 from  '+@staging_schema+'.'+@viewname+' as S  Where S.'+@businesskey+' In ( Select '+@businesskey+' from '+@prodschema+'.'+@tablename+' ) ;
'                                   	 
if @debug = 1 print @step2
 else 
exec sp_executesql  @step2
							 set @insertupdatedrecords = @@rowcount

--================Step 3 Insert New Records    
declare @Step3 nvarchar(max) =  
'
	 Insert Into '+@prodschema+'.'+@tablename+' ('+@targetcolumns+',File_Seq_No)   	
	 select '+@targetcolumns+','''+@fileseqno+'''
	 from  '+@staging_schema+'.'+@viewname+' as S  Where S.'+@businesskey+' Not In ( Select '+@businesskey+' from '+@prodschema+'.'+@tablename+' ) ;
'                                   	 
if @debug = 1 print @Step3
 else 
exec sp_executesql  @Step3
							 set @insertnewrecords = @@rowcount

--===============Step 4 Update Lastest Records Indicator to 1  
declare @step4 nvarchar(max) = 
'	
	 Update  T	set T.Valid_From = ''' + convert(varchar(25),@Load_Date,121) + ''' , T.File_Seq_No = '''+@fileseqno+'''
	 from  '+@prodschema+'.'+@tablename+'  as T	 
	 Join  '+@staging_schema+'.'+@viewname+' as S On T.'+@businesskey+' = S.'+@businesskey+' AND T.Hash = S.Hash
	 Where T.Latest_Indicator = 1 ;
'
if @debug = 1 print @step4
 else 
exec sp_executesql  @step4                          
							-- set @updateinprod2 = @@rowcount                     

--==============Table Load count update======================================================= 
Set @Records_Inserted = Cast(Cast(@insertnewrecords as Int) as varchar(max))
Set @Records_Updated  = Cast(Cast(@insertupdatedrecords as Int)  as varchar(max))
set @Records_Deleted  = Cast(Cast(@deleteinstag as Int) as varchar(max))
set @Records_Deleted_Staging = Cast((Cast(@deleteghostrecordsstag as Int) + cast(@deleteemptyrows as int)) as varchar(max))
set @Records_Duplicated =  Cast(Cast(@deletedups as Int) as varchar(max))
set @Versions_Updated = Cast(Cast(@Versions_Updated as Int) + cast(@Records_Deleted as Int) as varchar(max))
---Updating Files_To_Load Table with record Count-----
 Declare @sql8 nvarchar(max) =' 
 Update [Fileloader].[Files_To_Load] Set Load_Flg = 1 ,Records_Updated = cast('+@Records_Updated+' as int) ,Records_Inserted = cast('+@Records_Inserted+' as int) ,Records_Deleted = cast('+@Records_Deleted+' as int) ,Records_Deleted_Staging = cast('+@Records_Deleted_Staging+' as int) ,Records_Duplicated = cast('+@Records_Duplicated+' as int) ,Versions_Updated = cast('+@Versions_Updated+' as int) ,End_Date_Time = dateadd(ss,1,getdate())  Where Sr_No =  cast('+@Sr_No1+' as int)
 '
if @debug = 1 Print @sql8 
else 
exec sp_executesql @sql8
 
-- Update file to load history
         Exec [Fileloader].[Load_File_To_Load_History]
Commit Tran

End Try

Begin Catch
IF (XACT_STATE() != 0) 

Rollback Tran;
--=========recording error in files to load table 
Update [Fileloader].[Files_To_Load] Set [Error_Message] = 'Error At insertintoprodtable :'+@businessKeyMissingError+' '+ERROR_MESSAGE() , [File_Name_No_Extension] = ''+@filenamenoextension+''  Where Sr_No =  cast(''+@Sr_No+'' as int)
 
end Catch
GO


/****** Object:  StoredProcedure [Fileloader].[Sp_Matching_Column_With_DataType]    Script Date: 29/01/2024 10:22:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [Fileloader].[Sp_Matching_Column_With_DataType]						
 								
@SourceSchema Nvarchar(max) ,							
@SourceObject Nvarchar(max) ,
@TargetSchema Nvarchar(max) ,									
@TargetObject Nvarchar(max) 							
as 								
Begin								
								
IF 1 = 1						
								
Begin								
--declare @SourceSchema Nvarchar(max) = 'Staging_Horizon'								
--declare @SourceObject Nvarchar(max) = 'V_abc'
--declare @TargetSchema Nvarchar(max) = 'Staging_Horizon'									
--declare @TargetObject Nvarchar(max) = 'abc'	
declare @TargetObjectwithTargetSchema Nvarchar(max) = @TargetSchema+'.'+@TargetObject	
--print 	@TargetObjectwithTargetSchema				
Declare  @COLUMN_NAME nvarchar(max),@DATA_TYPE nvarchar(max),@CHARACTER_MAXIMUM_LENGTH nvarchar(max),@Sql nvarchar(max)  = ''								
								
declare  ColumnCheck Cursor  								
								
---------------------------------------------Enter Source Table Information-----------------------------------------------------------------								
for  ( select s.COLUMN_NAME,s.DATA_TYPE,case when  cast(s.CHARACTER_MAXIMUM_LENGTH as nvarchar(max)) =  CAST(-1 as nvarchar(max))  then 'Max'								
                                                     else cast(s.CHARACTER_MAXIMUM_LENGTH as nvarchar(max))								
                                                        end as CHARACTER_MAXIMUM_LENGTH 								
         from INFORMATION_SCHEMA.COLUMNS s where Table_Name =  @SourceObject AND TABLE_SCHEMA = @SourceSchema ) 								
--------------------------------------------------------------------------------------------------------------------------------------------         								
         								
open ColumnCheck								
								
Fetch Next from ColumnCheck into @COLUMN_NAME,@DATA_TYPE,@CHARACTER_MAXIMUM_LENGTH								
								
while  @@FETCH_STATUS = 0								
								
begin  								
								
								
/*-----------------------------------------Non Exist Columns-------------------------------------------------------------------------------------*/								
								
/*----Not Null Column Lenght data types */								
 if @CHARACTER_MAXIMUM_LENGTH is not null 								
 								
   Begin								
  								
         Begin try								
                      if not exists 								
                       (select 1 from INFORMATION_SCHEMA.COLUMNS T   								
                         where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema )								
                          								
						 Begin		
						   Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' add '+quotename(@COLUMN_NAME)+' '+@DATA_TYPE+'('+@CHARACTER_MAXIMUM_LENGTH+')'		
		  						
							       --Print (@Sql)	
							       Exec (@Sql)     	
						  End		
                    								
          END TRY								
             								
		  Begin catch						
		  SELECT ERROR_MESSAGE() AS 'Message' 						
		  end catch                          						
                                    								
    End								
 								
 								
 								
/*---- Null Column Lenght data types*/ 								
 if @CHARACTER_MAXIMUM_LENGTH is null 								
 								
   Begin								
  								
       begin try								
                      if not exists 								
                       (select 1 from INFORMATION_SCHEMA.COLUMNS T   								
                         where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema)								
                          								
						 Begin		
						   Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' add '+quotename(@COLUMN_NAME)+' '+@DATA_TYPE+''		
		  						
							       --Print (@Sql)	
							       Exec (@Sql)     	
						  End		
                    								
           END TRY								
             								
		  begin catch						
		  SELECT ERROR_MESSAGE() AS 'Message' 						
		  end catch                             						
                                    								
    End								
               								
 								
/*-------------------------------------Existing Columns with different Data Types--------------------------------------------------------------*/ 								
                                								
/*----Not Null Column Lenght data types */                        								
 if @CHARACTER_MAXIMUM_LENGTH is not null 								
 								
   Begin								
								
								
         begin try								
                      if exists 								
                       (select 1 from INFORMATION_SCHEMA.COLUMNS T   								
                         where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema )								
                          								
						 Begin		
						   Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' alter  Column  '+quotename(@COLUMN_NAME)+'  '+@DATA_TYPE+'('+@CHARACTER_MAXIMUM_LENGTH+')'		
		  						
							      --Print (@Sql)	
							      Exec (@Sql)     	
						  End		
                    								
           END TRY								
             								
		  begin catch						
		  SELECT ERROR_MESSAGE() AS 'Message' 						
		  end catch                          						
                                    								
    End  								
 								
 								
 								
 -- Null Column Lenght data types 								
 if @CHARACTER_MAXIMUM_LENGTH is null 								
 								
	   Begin							
	  							
		   begin try						
						  if exists 		
						   (select 1 from INFORMATION_SCHEMA.COLUMNS T   		
							 where Table_Name =  @TargetObject and COLUMN_NAME = @COLUMN_NAME AND TABLE_SCHEMA = @TargetSchema)	
	                          							
							 Begin	
								Set  @Sql = 'Alter Table '+@TargetObjectwithTargetSchema+' alter  Column  '+quotename(@COLUMN_NAME)+'  '+@DATA_TYPE+''
			  					
			  					
								      --Print (@Sql)
								      Exec (@Sql)     
							  End	
	                    							
			   END TRY					
	             							
			  begin catch					
			  SELECT ERROR_MESSAGE() AS 'Message' 					
			  end catch                             					
	                                    							
		End						
	           							
								
Fetch Next from ColumnCheck into   @COLUMN_NAME,@DATA_TYPE,@CHARACTER_MAXIMUM_LENGTH								
end 								
								
								
close ColumnCheck 								
								
deallocate ColumnCheck 								
  								
								
End								
								
								
else								
								
SELECT ERROR_MESSAGE() AS 'Message'								
								
						
  								
  								
End
GO
ALTER DATABASE [test] SET  READ_WRITE 
GO
