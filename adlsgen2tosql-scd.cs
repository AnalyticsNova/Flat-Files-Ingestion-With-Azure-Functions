using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using Azure.Storage.Files.DataLake;
using Azure.Storage;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace adlsgen2tosql
{
    public static class adlsgen2tosql
    {


        //============Application Instructions:===========================================================================================================================================================================================================
        //=1) Input Parameter values where required only.
        //=2) do not change values where it specifies. 

        //=================Data lake Connection Parameters//
        public static string tenantId = "";
        public static string applicationId = "";
        public static string clientSecret = "";
        public static string subscriptionId = "";
        public static string sqlconnectionstring = System.Environment.GetEnvironmentVariable("AzureSqlConnection");    // enter sql connection  string 
        public static string storageAccountName = System.Environment.GetEnvironmentVariable("storageAccountName");   // adls account name
        public static string storageAccountKey = System.Environment.GetEnvironmentVariable("storageAccountKey");   // storage account key

        public static string Staging_Schema = "staging";                                   // enter staging schema, prod schema will be created automatically 
        public static string source_container = "scd";                                      //enter source folder
        public static string archive_container = "archive";                                 //enter archive folder

        public static string localFileTransferPath = @"C:\Users\";                                // enter only if file require to copy from local directory 
        public static string remoteFileTransferPath = @"/test";                              // enter only if file require to copy from local directory
        //=============== Do not Change below parameter values//
        public static string StagingColumnsDataType = "Nvarchar(4000)";               // do not change
        public static string datetime = DateTime.Now.ToString("yyyyMMddHHmmss");        // do not change
        public static string datetime2 = DateTime.Now.ToString("yyyyMMdd");         // do not change
        public static int rowcnt = 0;                                              // do not change 
        public static string Prod_Schema = "dbo";                                  // do not change
        public static int Sr_No = 0;                                             // do not change
        //public static char   datatabledelimiter = ',';                            // do not change
        public static string FileWordToRemove = @"scd/";                            // do not change
        public const string blobtriggerpath = "test/{name}";
        public static string archive_directory = Prod_Schema + "_" + datetime2;
        public static IList<char> delimiter_char = new List<char>() {',','\t','\n','|',';'};
        public static IList<string> delimiter_str = new List<string>() {",","\t","|",";"};
        public static string fileseqno { get; set; }
        public static int    filerowcounts { get; set; }
        public static string filename { get; set; }
        public static string filefullpath { get; set; }
        public static string fileuri { get; set; }
        public static string file_header { get; set; }
        public static string column_list_with_StagingColumnsDataType { get; set; }
        public static string file_delimiter { get; set; }                                         // enter file delimiter  
        public static string fileextension { get; set; }
        public static string fileextentionnodot { get; set; }
        public static string filenamenoextension { get; set; }
        public static string tablename { get; set; }
        public static string fileid {get;set;}
    //==================================================================================================================================================================================================================================================

    [Singleton(Mode =SingletonMode.Function)]
    [Singleton(Mode = SingletonMode.Listener)]
    [FunctionName("adlsgen2tosql")]
        public static async Task RunAsync([BlobTrigger(blobPath:"scd/{name}",Connection = "AzureWebJobsStorage")] Stream data, string name, string blobTrigger, Uri Uri, IDictionary<string, string> Metadata, ILogger log,
                                          [Queue(queueName:"deadletterqueue",Connection = "AzureWebJobsStorage")] IAsyncCollector<DeadLetterMessage> deadLetterMessages
                                 )
        {
            var config = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory())
                 .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                 .AddEnvironmentVariables()
                 .Build();

            // connect to adls gen2 for file operation
            // connect to adls file system using account key
            string dfsUri = "https://" + storageAccountName + ".dfs.core.windows.net";
            StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(storageAccountName, storageAccountKey);
            // Create DataLakeServiceClient using StorageSharedKeyCredentials
            DataLakeServiceClient serviceClient = new DataLakeServiceClient(new Uri(dfsUri), sharedKeyCredential);
            //Console.WriteLine($"adlsname:{storageAccountName} \n adlasaccountkey:{storageAccountKey}");


            try
            {
                //StreamReader reader = new StreamReader(data);
                //string filecontent = reader.ReadToEnd();
                //log.LogInformation($"filecontent:{filecontent}");
               
                // variables created from blob input binding variables
                filename = name;
                filefullpath = blobTrigger;
                fileuri = Uri.ToString();
                // Get file extension   
                FileInfo fi = new FileInfo(filename);
                fileextension = fi.Extension;
                fileextentionnodot = fileextension.Replace(".", "");
                //Console.WriteLine("File Extension: {0}", fileextension);            
                filenamenoextension = filename.Replace(fileextension, "");
                System.Text.RegularExpressions.Regex rgx = new System.Text.RegularExpressions.Regex("[^a-zA-Z]");
                tablename = (rgx.Replace(filename.Replace(FileWordToRemove, ""), "").Replace(fileextension, "").Replace(fileextentionnodot, ""));
                var guid = Guid.NewGuid();
                fileid = guid.ToString();
                var data_table_name = filename + fileid;
 
                //--detect file delimiter 
                //data.Position = 0;
                //var requestBody = new StreamReader(data);
                var filedel = detectdelimiter(data,3, delimiter_char);



                //- file content into a data table

                DataTable datatable = AddNulls(createdatatablefromfile(data, data_table_name,filedel));
                DataSet ds = new DataSet();
                ds.Tables.Add(datatable);
                DataTable dt1 = ds.Tables[data_table_name];

                //--generate file headers
                file_header = "" ;
                var filehead = getfileheaderinfo(dt1,filedel);
                //var filehead_info = "[" + filehead.Replace(filedel, "],[") + "]";
                Console.WriteLine($"This is file header from file generated:{filehead}");

                //--insert file audit info into sql
                insertfileauditinfo(filename, tablename, filenamenoextension, filehead, filefullpath, 0, Prod_Schema, datetime, fileid, filedel);


                DataTableCollection collection = ds.Tables;
                    for (int i = 0; i < collection.Count; i++)
                    {
                        DataTable dt = collection[i];
                        //Console.WriteLine("{0}: {1}", i, collection[i]);


                    // Write row count of table.
                    rowcnt = collection[i].Rows.Count;

                    //Console.WriteLine("table_name: {0}",dt.TableName);
                    //Console.WriteLine("rowcount: {0}", rowcnt);
                    //printdtvalues(collection[i]);


                    ////--Get file information to load 
                    DataTable table = Getfilestoloadinformation(collection[i].ToString());
                    //foreach (DataRow dr in table.Rows)
                    //{
                        DataRow dr = table.Rows[0];
                        filename = dr["File_Name"].ToString();
                        filefullpath = dr["File_Source_Folder"].ToString();
                        filenamenoextension = dr["File_Name_No_Extension"].ToString();
                        tablename = dr["Table_Name"].ToString();
                        //string stagingviewname = "V" + "_" + tablename;
                        fileseqno = dr["Seq_No"].ToString();
                        Sr_No = Convert.ToInt32(dr["Sr_No"]);
                        fileid = dr["File_ID"].ToString();
                        file_header = dr["Table_Definition"].ToString();
                        file_delimiter = dr["File_Delimiter"].ToString();

                    //printdtvalues(table);
                    ////log.LogInformation($"C# Blob trigger function Processed blob\n Name:{filename} \n table_name:{tablename} \n Size: {data.Length} Bytes \n Full blob path: {blobTrigger} \n uri: {fileuri} \n file_extention:{fileextension} \n fileextentionnodot = {fileextentionnodot} \n fileseqno:{fileseqno} \n Sr_no: {Sr_No} ");



                    ////--generate file headers
                    //file_header = "";
                    //getfileheaderinfo(dt);

                    //--create staging and prod schema
                    createstagingandProdSchema(Staging_Schema, Prod_Schema);

                    //--Populating staging tables / uncomment code inside method if you like to truncate table 
                    createstagingtableifnotexist(file_header, Staging_Schema, filefullpath, tablename);

                    //--insert into staging table
                    insertintostagingtable(collection[i], tablename, Staging_Schema, filenamenoextension, filename, filefullpath);

                    //--generate column list with generic data type and inclose them in brackets 
                    column_list_with_StagingColumnsDataType = "[" + file_header.Replace(file_delimiter, $"]{" " + StagingColumnsDataType},[") + "]" + " " + StagingColumnsDataType;
                    var ColumnList = "[" + file_header.Replace(file_delimiter, "],[") + "]";
                    //Console.WriteLine($"column_list_with_data_type:{column_list_with_StagingColumnsDataType}");
                    //Console.WriteLine($"columnlist_with_brackets{ ColumnList}");

                    //--Create staging views with hash column
                    createstagingviews(ColumnList, column_list_with_StagingColumnsDataType, Staging_Schema, Prod_Schema, tablename, filename, filefullpath);

                    //--Create prod tables
                    createprodtablesifnotexist(tablename, column_list_with_StagingColumnsDataType, Staging_Schema, Prod_Schema, filename, filefullpath);

                    //--Matching staging view definition with prod table
                    AddMissingColumnsWithDataType(Staging_Schema, tablename, Prod_Schema, tablename);

                    //--Insert into prod table
                    insertintoprodtable(tablename, Staging_Schema, Prod_Schema, filenamenoextension, Sr_No, filename, filefullpath, fileseqno, rowcnt, ColumnList);

                    //--Drop staging view and staging table
                    dropstagingviewsandtable(tablename, Staging_Schema);

                    //--Create Archive folder with date time
                    createarchivedirectorycontainer(serviceClient);

                    //--Moving file to archive folder
                    //archivefile(serviceClient, filename, source_container, archive_container, archive_directory);

                    //--Create Prod View
                    createprodviews(Prod_Schema, tablename, filename, filefullpath);


                    // //--clear objects
                    collection[i].Clear();
                    // //file_header = "";
                    // //ColumnList = "";
                    // //column_list_with_StagingColumnsDataType = "";

                    //}
       
                    //string json = @"{""data"":[{""id"":""518523721"",""name"":""ftyft""}, {""id"":""527032438"",""name"":""ftyftyf""}, {""id"":""527572047"",""name"":""ftgft""}, {""id"":""531141884"",""name"":""ftftft""}]}";
                    //string output = JsonConvert.SerializeObject(json);
                    //await deadLetterMessages.AddAsync(new DeadLetterMessage { filename = filename, filelocation = filefullpath });
                }

                //--clear objects
                //ds.Clear();

                


            }

            catch (Exception e) //Main Method exeption
            {
                log.LogError(e.ToString());
                Console.WriteLine(e);
                await deadLetterMessages.AddAsync(new DeadLetterMessage { Issue = e.Message, filename = filename, filelocation = filefullpath });

            }


        }


        //== create pro view
        public static void createprodviews(string prod_schema, string tablename, string filename, string filesource)
        {
            try
            {
                // executing sp to create view
                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Create_Production_View", conn);
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add("@Prod_schema", SqlDbType.VarChar).Value = prod_schema;
                    cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = tablename;
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine($"16-createprodviews finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
            }

            // writting exceptions to audit table [Fileloader].[Files_To_Load] 
            catch (Exception e) //catch log exceptions for each file 
            {

                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Files_To_Load", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@Fileid", SqlDbType.NVarChar).Value = "";
                    cmd.Parameters.Add("@Filename", SqlDbType.VarChar).Value = filename;
                    cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Filenamenoextension", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@tabledefinition", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Validationflg", SqlDbType.VarChar).Value = "Failure";
                    cmd.Parameters.Add("@filesource", SqlDbType.VarChar).Value = filesource;
                    cmd.Parameters.Add("@filerowcount", SqlDbType.Int).Value = 0;
                    cmd.Parameters.Add("@errormessage", SqlDbType.VarChar).Value = "Error At createprodviews :" + e.Message.ToString();
                    cmd.Parameters.Add("@SchemaName", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedatetime", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedelimiter", SqlDbType.NVarChar).Value = "";
                    // Output Sr_No for Update
                    SqlParameter outputParam = cmd.Parameters.Add("@Sr_No", SqlDbType.Int);
                    outputParam.Direction = ParameterDirection.Output;
                    cmd.CommandTimeout = 250000;
                    cmd.ExecuteNonQuery();
                }

            }

        }

        //// create directory in ADLS
        public static void createarchivedirectorycontainer(DataLakeServiceClient serviceClient)

        {

            // Create a DataLake Filesystem (container)
             serviceClient.CreateFileSystemAsync(archive_container);
            //get reference to the container
             DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(archive_container);
            fileSystemClient.CreateIfNotExists();
            //create a directory(folder) inside the container 
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(archive_directory);
            directoryClient.CreateIfNotExists();


            Console.WriteLine($"14-createdirectory file finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        }


        //// archive files in ADLS
        public static void archivefile(DataLakeServiceClient client, string filename, string source_container, string archive_container, string archive_directory)
        {

            var SouceFileFullPath = filefullpath;
            var DestFileFullPath = (archive_container + @"/" + archive_directory + @"/" + filename);

            var fileSystemClient_source = client.GetFileSystemClient(source_container);
            var fileClient = fileSystemClient_source.GetFileClient(filename);
            var fileSystemClient_archive = client.GetFileSystemClient(archive_container);
            var directoryClient_archive = fileSystemClient_archive.GetDirectoryClient(archive_directory);
           // fileClient.RenameAsync(directoryClient_archive.ToString());

            Console.WriteLine($"14-archiving file finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        }



        // drop staging table and view
        public static void dropstagingviewsandtable(string tablename, string staging_schema)
        {
            try
            {
                string dropstagingview  = "Drop View IF EXISTS " + staging_schema + ".[" + "V_" + tablename + "];";
                string dropstagingtable = "Drop Table IF EXISTS " + staging_schema + ".[" + tablename + "];";
                //Console.WriteLine(dropstagingtable.ToString()+"\n"+dropstagingview.ToString());
                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmddropview = new SqlCommand(dropstagingview, conn);
                    cmddropview.ExecuteNonQuery();
                    SqlCommand cmddroptable = new SqlCommand(dropstagingtable, conn);
                    cmddroptable.ExecuteNonQuery();
                }
                Console.WriteLine($"13-dropstagingviewsandtable finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
            }
            catch (Exception e)
            { Console.WriteLine(e.ToString()); }
        }

        // create prod tables 
        public static void insertintoprodtable(string tablename,string Staging_Schema,string prod_schema, string filenamenoextension, int Sr_No, string filename, string filesource, string fileseqno,int filerowcount , string fileheader)
        {
            try
            {

                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Insert_Into_Production_Table", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@tablename", SqlDbType.VarChar).Value = tablename;
                    cmd.Parameters.Add("@staging_schema", SqlDbType.VarChar).Value = Staging_Schema;
                    cmd.Parameters.Add("@prod_schema", SqlDbType.VarChar).Value = prod_schema;
                    cmd.Parameters.Add("@filenamenoextension", SqlDbType.VarChar).Value = filenamenoextension;
                    cmd.Parameters.Add("@fileseqno", SqlDbType.NVarChar).Value = fileseqno;
                    cmd.Parameters.Add("@FileRowCount", SqlDbType.Int).Value = filerowcount;
                    cmd.Parameters.Add("@fileheader", SqlDbType.NVarChar).Value = fileheader;
                    //cmd.Parameters.Add("@ArchiveFolder", SqlDbType.VarChar).Value = archivefolder+@"/"+filename.Replace("'","");
                    // update File_To_Load table with table load count
                    cmd.Parameters.Add("@Sr_No", SqlDbType.Int).Value = Sr_No;
                    cmd.CommandTimeout = 500000000;
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine($"12-insertintoprodtable finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");

            }

            catch (Exception e)
            {
                Console.Write(e.Message);
                // writting exceptions to audit table [Fileloader].[Files_To_Load] 
                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Files_To_Load", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@Fileid", SqlDbType.NVarChar).Value = "";
                    cmd.Parameters.Add("@Filename", SqlDbType.VarChar).Value = filename.ToString().Replace("'", "");
                    cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Filenamenoextension", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@tabledefinition", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Validationflg", SqlDbType.VarChar).Value = "Failure";
                    cmd.Parameters.Add("@filesource", SqlDbType.VarChar).Value = filesource;
                    cmd.Parameters.Add("@filerowcount", SqlDbType.Int).Value = 0;
                    cmd.Parameters.Add("@errormessage", SqlDbType.VarChar).Value = "Error At insertintoprodtable :" + e.Message.ToString();
                    cmd.Parameters.Add("@SchemaName", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedatetime", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedelimiter", SqlDbType.NVarChar).Value = "";
                    // Output Sr_No for Update
                    SqlParameter outputParam = cmd.Parameters.Add("@Sr_No", SqlDbType.Int);
                    outputParam.Direction = ParameterDirection.Output;
                    cmd.CommandTimeout = 25000000;
                    cmd.ExecuteNonQuery();


                }

            }

        }


        // mathcing staging view columns to prod table
        public static void AddMissingColumnsWithDataType(string staging_schema, string SourceObject, string target_schema, string TargetObject)
        {
            using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand("Fileloader.Sp_Add_Missing_Columns_With_DataType", conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@SourceSchema", SqlDbType.VarChar).Value = staging_schema;
                cmd.Parameters.Add("@SourceObject", SqlDbType.VarChar).Value = "V_" + SourceObject;
                cmd.Parameters.Add("@TargetSchema", SqlDbType.VarChar).Value = target_schema;
                cmd.Parameters.Add("@TargetObject", SqlDbType.VarChar).Value = TargetObject;
                cmd.CommandTimeout = 25000000;
                cmd.ExecuteNonQuery();
            }

            Console.WriteLine($"11-AddMissingColumnsWithDataType finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        }

        public static DataTable createdatatablefromfile(Stream data, string datatablename,string filedel)
        {

            data.Position = 0;
            DataTable dt = new DataTable();
            dt.TableName = datatablename;
            //dt.Clear();
            
            using (StreamReader sr = new StreamReader(data))
            {

                string[] headers = sr.ReadLine().Split(filedel.ToCharArray());
                foreach (string header in headers)
                {
                    dt.Columns.Add(header.Trim('\"')); // Quotes removed
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(filedel.ToCharArray());
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i].Trim('\"'); // Quotes removed
                    }
                    dt.Rows.Add(dr);
                }

            }

            // Console.WriteLine(Sr_No.ToString());
            Console.WriteLine($"4-createdatatablefromfile finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
            return dt;
        }

         
        // replacing blank values with null
        public static DataTable AddNulls(DataTable dt)
        {
            //Console.WriteLine("enter into AddNulls");
            for (int a = 0; a < dt.Rows.Count; a++)
            {
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    if (dt.Rows[a][i].ToString() == "")
                    {
                        dt.Rows[a][i] = DBNull.Value;
                    }
                }
            }
           
            return dt;
        }


        //====populate staging tables 
        public static void createstagingtableifnotexist(string fileheader,string staging_schema, string filefullpath, string tablename)

        {

            //Console.WriteLine("Inside createtableifnotexist");
 //           using (var readStream = new StreamReader(data))

 //           {
                string TableName = tablename;
                string line;
                //string ColumnList = "";
                // reading header line of file
                line = fileheader;
                //ColumnList = "[" + line.Replace(file_delimiter, "],[") + "]";

            //====drop staging Table if exist
            string DropTableStatement = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[" + Staging_Schema + "].";
            DropTableStatement += "[" + TableName + "]')";
            DropTableStatement += " AND type in (N'U')) ";
            DropTableStatement += "Drop Table [" + Staging_Schema + "].[" + TableName + "]";
            //Console.WriteLine(DropTableStatement.ToString());
            //====Create Table if not exists============================================================================================= 
            string CreateTableStatement = "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[" + Staging_Schema + "].";
                CreateTableStatement += "[" + TableName + "]')";
                CreateTableStatement += " AND type in (N'U')) ";
                CreateTableStatement += "Create Table [" + Staging_Schema + "].[" + TableName + "]";
                CreateTableStatement += "([" + line.Replace(file_delimiter, "] " + StagingColumnsDataType + ",[") + "] " + StagingColumnsDataType + ")";
               // Console.WriteLine(CreateTableStatement.ToString());
                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand DropTableCmd = new SqlCommand(DropTableStatement, conn);
                    DropTableCmd.ExecuteNonQuery();

                    SqlCommand CreateTableCmd = new SqlCommand(CreateTableStatement, conn);
                    CreateTableCmd.ExecuteNonQuery();
                }

            //====Truncate Table if already exists
            //string query = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[" + Staging_Schema + "].";
            //query += "[" + TableName + "]')";
            //query += " AND type in (N'U')) ";
            //query += "Truncate Table " + Staging_Schema + ".[" + TableName + "];";
            ////Console.WriteLine(query.ToString());
            //using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
            //{
            //    conn.Open();
            //    SqlCommand myCommand1 = new SqlCommand(query, conn);
            //    myCommand1.ExecuteNonQuery();
            //}

            //            }


            Console.WriteLine($"7-createstagingtableifnotexist finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        }


    // create prod and staging schema
    public static void createstagingandProdSchema(string staging_schema ,string prod_schema)
        {
            
            //staging schema
            string createstagingschema = "if not exists (select * from sys.schemas where name = " + "'" + staging_schema + "'" + ")";
            createstagingschema += " exec ('Create Schema " + staging_schema + "')";
            //prod schema
            string createProdSchema = "if not exists (select * from sys.schemas where name = " + "'" + prod_schema + "'" + ")";
            createProdSchema += " exec ('Create Schema " + prod_schema + "')";

            //Console.WriteLine(createschema.ToString() +"\n"+ createProdSchema.ToString());
            using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
            {
                conn.Open();
                SqlCommand cmdschemaname = new SqlCommand(createstagingschema, conn);
                cmdschemaname.ExecuteNonQuery();

                SqlCommand cmdProdSchema = new SqlCommand(createProdSchema, conn);
                cmdProdSchema.ExecuteNonQuery();
            }

            Console.WriteLine($"6-createstagingandProdSchema finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        }

        //======Insert flat files data into staging tables using data table created above
        public static void insertintostagingtable(DataTable dataTableName, string tablename, string schemaname, string filenamenoextension, string filename, string filesource)
        {
            //Console.WriteLine("enter into insertintostagingtable ");
            string tablenamewithschema = $"{Staging_Schema + "." + tablename}";
            //string tablenamewithschema = "staging.test";
            //Console.WriteLine(tablenamewithschema);
            //using (SqlConnection connection = new SqlConnection(sqlconnectionstring))
            //{
            //    connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlconnectionstring))
                {

                    bulkCopy.BulkCopyTimeout = 25000000;
                    bulkCopy.DestinationTableName = tablenamewithschema;
                    try
                    {
                        bulkCopy.WriteToServer(dataTableName);
                    }
                    catch (Exception e)
                    {
                        Console.Write(e.Message);
                   // writting exceptions to audit table[Fileloader].[Sp_Files_To_Load]
                        using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                    {
                        conn.Open();
                        SqlCommand cmd = new SqlCommand("Fileloader.Sp_Files_To_Load", conn);
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add("@Fileid", SqlDbType.NVarChar).Value = "";
                        cmd.Parameters.Add("@Filename", SqlDbType.VarChar).Value = filename.ToString().Replace("'", "");
                        cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = tablename.ToString().Replace("'", "");
                        cmd.Parameters.Add("@Filenamenoextension", SqlDbType.VarChar).Value = "";
                        cmd.Parameters.Add("@tabledefinition", SqlDbType.VarChar).Value = "";
                        cmd.Parameters.Add("@Validationflg", SqlDbType.VarChar).Value = "Failure";
                        cmd.Parameters.Add("@filesource", SqlDbType.VarChar).Value = filesource;
                        cmd.Parameters.Add("@filerowcount", SqlDbType.Int).Value = 0;
                        cmd.Parameters.Add("@errormessage", SqlDbType.VarChar).Value = "Error At insertintostagingtable :" + e.Message.ToString();
                        cmd.Parameters.Add("@SchemaName", SqlDbType.VarChar).Value = "";
                        cmd.Parameters.Add("@filedatetime", SqlDbType.VarChar).Value = "";
                        cmd.Parameters.Add("@filedelimiter", SqlDbType.NVarChar).Value = "";
                        // Output Sr_No for Update
                        SqlParameter outputParam = cmd.Parameters.Add("@Sr_No", SqlDbType.Int);
                        outputParam.Direction = ParameterDirection.Output;
                        cmd.ExecuteNonQuery();

                        //int id = outputParam.Value;
                    }

                }

                }

            //}
         
               Console.WriteLine($"8-insertintostagingtable finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        }

        // get table headers from dt
        //public static DataRow getfileheaderinfo(DataTable dt)
        //{

        //    DataRow row = dt.NewRow();
        //    DataColumnCollection columns = dt.Columns;
        //    for (int i = 0; i < columns.Count; i++)
        //    {
        //        row[i] = columns[i].ColumnName;

        //        //Console.WriteLine("[{0}]", string.Join(",", row[i]));
        //        file_header += (string.Join(file_delimiter, row[i]+file_delimiter));
        //        //column_list_with_StagingColumnsDataType += (string.Join(file_delimiter, row[i] + file_delimiter));
        //    }

        //    file_header = file_header.Substring(0, file_header.Length - 1);
        //    //Console.WriteLine($"file_header:{file_header}");
        //    Console.WriteLine($"5-getfileheaderinfo finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        //    return row;
        //}

        public static string getfileheaderinfo(DataTable dt,string filedelimiter)
        {
            var fileheader = "";
            DataRow row = dt.NewRow();
            DataColumnCollection columns = dt.Columns;
            for (int i = 0; i < columns.Count; i++)
            {
                row[i] = columns[i].ColumnName;

                //Console.WriteLine("[{0}]", string.Join(",", row[i]));
                fileheader += (string.Join(filedelimiter, row[i] + filedelimiter));
                //column_list_with_StagingColumnsDataType += (string.Join(file_delimiter, row[i] + file_delimiter));
            }

            fileheader = fileheader.Substring(0, fileheader.Length - 1);
            //fileheader = "[" + fileheader.Replace(fileheader, "],[") + "]";
            //Console.WriteLine($"file_header:{file_header}");
            Console.WriteLine($"5-getfileheaderinfo finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
            return fileheader;
        }

        private static void printdtvalues(DataTable table)
        {
            foreach (DataRow row in table.Rows)
            {
                foreach (DataColumn column in table.Columns)
                {
                    Console.WriteLine(row[column]);
                }

            }
        }

 


        public static string createtable(string sqlconnectionstring, string tableName,string schemaname, DataTable table)
        {
            string sqlsc;
            //using (System.Data.SqlClient.SqlConnection connection = new System.Data.SqlClient.SqlConnection(connectionString))
            using (SqlConnection connection = new SqlConnection(sqlconnectionstring))
            {
                connection.Open();
                sqlsc = "CREATE TABLE " + schemaname + "."+ tableName + "(";
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    sqlsc += "\n" + table.Columns[i].ColumnName;
                    if (table.Columns[i].DataType.ToString().Contains("System.Int32"))
                        sqlsc += " int ";
                    else if (table.Columns[i].DataType.ToString().Contains("System.DateTime"))
                        sqlsc += " datetime ";
                    else if (table.Columns[i].DataType.ToString().Contains("System.String"))
                        sqlsc += " nvarchar(" + table.Columns[i].MaxLength.ToString() + ") ";
                    else if (table.Columns[i].DataType.ToString().Contains("System.Single"))
                        sqlsc += " single ";
                    else if (table.Columns[i].DataType.ToString().Contains("System.Double"))
                        sqlsc += " double ";
                    else
                        sqlsc += " nvarchar(" + table.Columns[i].MaxLength.ToString() + ") ";



                    if (table.Columns[i].AutoIncrement)
                        sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                    if (!table.Columns[i].AllowDBNull)
                        sqlsc += " NOT NULL ";
                    sqlsc += ",";
                }

                string pks = "\nCONSTRAINT PK_" + tableName + " PRIMARY KEY (";
                for (int i = 0; i < table.PrimaryKey.Length; i++)
                {
                    pks += table.PrimaryKey[i].ColumnName + ",";
                }
                pks = pks.Substring(0, pks.Length - 1) + ")";

                sqlsc += pks;
                connection.Close();

            }
            return sqlsc + ")";
        }


  


        //== create staging views from staging tables with hash columns
        public static void createstagingviews(string fileheader,string column_list_with_StagingColumnsDataType, string Staging_Schema, string Prod_Schema, string tablename, string filename, string filesource)
        {
            try
            {
                // executing sp to create view
                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Create_Staging_View", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@file_header", SqlDbType.VarChar).Value = fileheader;
                    cmd.Parameters.Add("@column_list_with_StagingColumnsDataType", SqlDbType.VarChar).Value = column_list_with_StagingColumnsDataType;
                    cmd.Parameters.Add("@staging_schema", SqlDbType.VarChar).Value = Staging_Schema;
                    cmd.Parameters.Add("@prod_schema", SqlDbType.VarChar).Value = Prod_Schema;
                    cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = tablename;
                    cmd.ExecuteNonQuery();
                }

                Console.WriteLine($"9-createstagingviews finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
            }

            // writting exceptions to audit table [Fileloader].[Files_To_Load] 
            catch (Exception e) //catch log exceptions for each file 
            {

                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Files_To_Load", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@Fileid", SqlDbType.NVarChar).Value = "";
                    cmd.Parameters.Add("@Filename", SqlDbType.VarChar).Value = filename.ToString().Replace("'", "");
                    cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Filenamenoextension", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@tabledefinition", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Validationflg", SqlDbType.VarChar).Value = "Failure";
                    cmd.Parameters.Add("@filesource", SqlDbType.VarChar).Value = filesource;
                    cmd.Parameters.Add("@filerowcount", SqlDbType.Int).Value = 0;
                    cmd.Parameters.Add("@errormessage", SqlDbType.VarChar).Value = "Error At createstagingviews :" + e.Message.ToString();
                    cmd.Parameters.Add("@SchemaName", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedatetime", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedelimiter", SqlDbType.NVarChar).Value = "";
                    // Output Sr_No for Update
                    SqlParameter outputParam = cmd.Parameters.Add("@Sr_No", SqlDbType.Int);
                    outputParam.Direction = ParameterDirection.Output;
                    cmd.CommandTimeout = 250000;
                    cmd.ExecuteNonQuery();
                }

            }

        }


        // create prod tables 
        public static void createprodtablesifnotexist(string tablename, string column_list_with_StagingColumnsDataType, string staging_schema, string prod_schema, string filename, string filesource)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Create_Production_Table", conn);
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add("@tablename", SqlDbType.NVarChar).Value = tablename.ToString().Replace("'", "");
                    cmd.Parameters.Add("@column_list_with_StagingColumnsDataType", SqlDbType.NVarChar).Value = column_list_with_StagingColumnsDataType;
                    cmd.Parameters.Add("@staging_schema", SqlDbType.NVarChar).Value = staging_schema.ToString().Replace("'", "");
                    cmd.Parameters.Add("@prod_schema", SqlDbType.NVarChar).Value = prod_schema.ToString().Replace("'", "");
                    

                    cmd.ExecuteNonQuery();

                    Console.WriteLine($"10-createprodtablesifnotexist finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");

                }


            }

            catch (Exception e)

            {
                Console.Write(e.Message);
                // writting exceptions to audit table [Fileloader].[Files_To_Load] 
                using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("Fileloader.Sp_Files_To_Load", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@Fileid", SqlDbType.NVarChar).Value = "";
                    cmd.Parameters.Add("@Filename", SqlDbType.VarChar).Value = filename.ToString().Replace("'", "");
                    cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Filenamenoextension", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@tabledefinition", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@Validationflg", SqlDbType.VarChar).Value = "Failure";
                    cmd.Parameters.Add("@filesource", SqlDbType.VarChar).Value = filesource;
                    cmd.Parameters.Add("@filerowcount", SqlDbType.Int).Value = 0;
                    cmd.Parameters.Add("@errormessage", SqlDbType.VarChar).Value = "Error At createprodtablesifnotexist :" + e.Message.ToString();
                    cmd.Parameters.Add("@SchemaName", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedatetime", SqlDbType.VarChar).Value = "";
                    cmd.Parameters.Add("@filedelimiter", SqlDbType.NVarChar).Value = "";
                    // Output Sr_No for Update
                    SqlParameter outputParam = cmd.Parameters.Add("@Sr_No", SqlDbType.Int);
                    outputParam.Direction = ParameterDirection.Output;
                    cmd.CommandTimeout = 250000;
                    cmd.ExecuteNonQuery();
                }

            }

        }

        // drop staging table and view

        public static string detectdelimiter(Stream data, int rowCount, IList<char> separators)
        {
            var filedel = "";  
            // convert stream to string
             //data.Position = 0;
            StreamReader reader = new StreamReader(data);
           //string character = reader.ReadToEnd().ToCharArray;

            IList<int> separatorsCount = new int[separators.Count];

            int character;

            int row = 0;

            bool quoted = false;
            bool firstChar = true;

            while (row < rowCount)
            {
               character = reader.Read();

                switch (character)
                {
                    case '"':
                        if (quoted)
                        {
                            if (reader.Peek() != '"') // Value is quoted and 
                                                      // current character is " and next character is not ".
                                quoted = false;
                            else
                                reader.Read(); // Value is quoted and current and 
                                               // next characters are "" - read (skip) peeked qoute.
                        }
                        else
                        {
                            if (firstChar)  // Set value as quoted only if this quote is the 
                                            // first char in the value.
                                quoted = true;
                        }
                        break;
                    case '\n':
                        if (!quoted)
                        {
                            ++row;
                            firstChar = true;
                            continue;
                        }
                        break;
                    case -1:
                        row = rowCount;
                        break;
                    default:
                        if (!quoted)
                        {
                            int index = separators.IndexOf((char)character);
                            if (index != -1)
                            {
                                ++separatorsCount[index];
                                firstChar = true;
                                continue;
                            }
                        }
                        break;
                }

                if (firstChar)
                    firstChar = false;
            }

            int maxCount = separatorsCount.Max();
            
            // set file delimiter 
            if (maxCount != 0) { filedel = (separators[separatorsCount.IndexOf(maxCount)].ToString()).Trim();}
            if (filedel == "" || filedel == " ") { filedel = "\t";}

            //Console.WriteLine($"file_delimiter: {file_delimiter}");

            Console.WriteLine($"1-detectdelimiter finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
            // return maxCount == 0 ? '\0' : separators[separatorsCount.IndexOf(maxCount)];
            return filedel;
        }


        public static void insertfileauditinfo(string filename, string tablename, string filenamenoextension, string fileheader, string filesource,int filerowcount, string ProdSchema, string filedatetime,string fileid,string filedelimiter)
        {

            //using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
            //{
            //    conn.Open();
            //    SqlCommand Truncatetable = new SqlCommand("Truncate Table [Fileloader].[Files_To_Load]", conn);
            //    Truncatetable.ExecuteNonQuery();
            //}

            try //  log exceptions for each file 
                    {

                            // inserting file information into [Fileloader].[Files_To_Load]
                            using (SqlConnection conn = new SqlConnection(sqlconnectionstring))
                            {
                                conn.Open();
                                SqlCommand cmd = new SqlCommand("Fileloader.Sp_Files_To_Load", conn);
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.Parameters.Add("@Fileid", SqlDbType.NVarChar).Value = fileid;
                                cmd.Parameters.Add("@Filename", SqlDbType.VarChar).Value = filename.ToString().Replace("'", "");
                                cmd.Parameters.Add("@Tablename", SqlDbType.VarChar).Value = tablename;
                                cmd.Parameters.Add("@Filenamenoextension", SqlDbType.VarChar).Value = filenamenoextension;
                                cmd.Parameters.Add("@tabledefinition", SqlDbType.VarChar).Value = fileheader;
                                cmd.Parameters.Add("@Validationflg", SqlDbType.VarChar).Value = "Success";
                                cmd.Parameters.Add("@filesource", SqlDbType.VarChar).Value = filesource;
                                cmd.Parameters.Add("@filerowcount", SqlDbType.Int).Value = filerowcount;
                                cmd.Parameters.Add("@errormessage", SqlDbType.VarChar).Value = DBNull.Value;
                                cmd.Parameters.Add("@SchemaName", SqlDbType.VarChar).Value = ProdSchema;
                                cmd.Parameters.Add("@filedatetime", SqlDbType.VarChar).Value = filedatetime;
                                cmd.Parameters.Add("@filedelimiter", SqlDbType.NVarChar).Value = filedelimiter.ToString();
                                // Output Sr_No for Update
                                SqlParameter outputParam = cmd.Parameters.Add("@Sr_No", SqlDbType.Int);
                                outputParam.Direction = ParameterDirection.Output;
                                cmd.CommandTimeout = 250000;
                                cmd.ExecuteNonQuery();
                                // capturing output parameter value into local variable
                                Sr_No = (int)outputParam.Value;
                            }


                      

                    }

                    catch (Exception errors) //catch log exceptions for each file 
                    {

                        Console.WriteLine(errors.ToString());
             

                        
                    }


            Console.WriteLine($"2-insertfileauditinfo finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
        }


        public static int filerowcount(Stream data)
        {

            //data.Position = 0;
            using (var readStream = new StreamReader(data))

            {
                string line;
                int i = 0;
                while ((line = readStream.ReadLine()) != null) { i++; }
                
                return i - 1;
                
            }
           
        }

        public static int filelinecount(Stream data)
        {
            int lineCounter = 0;
            using (var reader = new StreamReader(data))
            {
                while (reader.ReadLine() != null)
                {
                    lineCounter++;
                }
                return lineCounter-1;
            }
    
        }

        public static  DataTable Getfilestoloadinformation(string datatablename)
        {
            string cs = sqlconnectionstring;
            using (SqlConnection con = new SqlConnection(cs))
            {
                con.Open();
                string sqltoexecute = " exec Fileloader.Sp_Get_Files_To_Load_Information '" + datatablename + "' ";
               // Console.WriteLine(datatablename);
                SqlCommand cmd = new SqlCommand(sqltoexecute, con);
                using (SqlDataReader rdr = cmd.ExecuteReader())
                {
                    DataTable table = new DataTable();
                    table.Columns.Add("Table_Name");
                    table.Columns.Add("File_Name");
                    table.Columns.Add("File_Name_No_Extension");
                    table.Columns.Add("File_Source_Folder");
                    table.Columns.Add("Table_Definition");
                    table.Columns.Add("Sr_No");
                    table.Columns.Add("Seq_No");
                    table.Columns.Add("File_Delimiter");
                    table.Columns.Add("File_ID");
                    

                    while (rdr.Read())
                    {
                        DataRow datarow = table.NewRow();
                        datarow["Table_Name"] = rdr["Table_Name"];
                        datarow["File_Name"] = rdr["File_Name"];
                        datarow["File_Name_No_Extension"] = rdr["File_Name_No_Extension"];
                        datarow["File_Source_Folder"] = rdr["File_Source_Folder"];
                        datarow["Table_Definition"] = rdr["Table_Definition"];
                        datarow["Sr_No"] = Convert.ToInt32(rdr["Sr_No"]);
                        datarow["Seq_No"] = rdr["Seq_No"];
                        datarow["File_Delimiter"] = rdr["File_Delimiter"];
                        datarow["File_ID"] = rdr["File_ID"];
                        table.Rows.Add(datarow);
                    }
                    Console.WriteLine($"3-Getfilestoloadinformation finished at :{DateTime.Now.ToString("yyyyMMddHHmmss")}");
                   return table;
                }
            }
        }

        public class DeadLetterMessage
        {
            public string Issue { get; set; }
            public string filename { get; set; }
            public string filelocation  { get; set; }
        }

        // end of program
    }

}
