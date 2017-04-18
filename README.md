# LargeData

Recently I was working with a problem, where we have to transfer 5 GB (maximum or first load) of data set across client (WPF client) and server (Web APIs) (both download and upload). All this need to happen over vpn connection and users are across the world with a varying internet speed (which sometimes could be as low as 16 kbps).

Initial implementation was to leverage the existing sync frameworks to synchronize the data and let the framework handle all the changes done by the user in the database. However, the sync framework was making things very slow because of database triggers (impacting inserts and updates, as in one transaction a maximum of 100K records could be inserted or updated)

Time taken by sync framework was approximately ~30 minutes to sync the entire data from server to client and  >1 hr to sync back from client to server.

To solve this performance bottleneck, first thing is to get rid of sync framework. but then again how do we keep track of changes without much changes in existing code (as everything else is working just fine except the data synchronization)

This part is not covered in the above library, that is, how to keep track of what changed.
So we already have a "rowversion" column in all the tables (we were using SQL server), which is always incremental across the DB. So to keep track of childs, what we need to do is to add a LastSyncedOn column in the parent table (for all the child table) and set LastSyncedOn to @DBTS. which means last time database was synced on that this timestamp and if any child table rowversion is greater than this value, then it a new row (either inserted or updated).

On the basis of this we created the DataSet, both at client and server. Feed the dataset to the above utility and send it across. 

After doing these changes, we were able to download the same data set in &lt;5 minutes and upload in &lt;10 minutes (depend upon the changes done in the data at the client side).

At some places, we also ended up using datareader, as dataset was giving out of memory exception.

#### Download flow

![Download process](https://raw.githubusercontent.com/lokeshlal/LargeData/master/download_process.png)

#### Upload flow

![Upload process](https://raw.githubusercontent.com/lokeshlal/LargeData/master/upload_process.png)


#### Server configuration

To include LargeData controller, please add following line in the Global file Application_Start() event

```csharp
GlobalConfiguration.Configuration.Services.Replace(typeof(IAssembliesResolver), new AssemblyResolver());
```

For download process

```csharp
// for data reader, set following property for download
ServerSettings.CallbackReader = DataProvider.GetDataReaderForDownload;
// for data set, set following property for download
ServerSettings.Callback = DataProvider.GetDataReaderForDownload;
```

For upload process

```csharp
// for data reader, set following property for upload
ServerSettings.CallbackUploadReader = DataReciever.AcceptDataReader;
// for data set, set following property for upload
ServerSettings.CallbackUpload = DataReciever.AcceptDataSet;
```

Staging location, where temporary files will be stored
```csharp
ServerSettings.TemporaryLocation = @"E:\TempLocation";
```

General server configuration
```csharp
// Maximum number of records in each file
ServerSettings.MaxRecordsInAFile = 10;
```

#### Client configuration

```csharp
// Either this can be set once or passed in Get/Set Data methods
// Rest API Base URI
ClientSettings.BaseUri = "http://api.base.com"
// Temporary location for staging files
ClientSettings.TemporaryLocation = @"E:\location"
```

#### Filter class

Use filter class to send additional information about upload and download.
Filters can be used in the Callbacks to retrieve the data and to process the data. Filter can have following (but not limited to)
1. User information
2. Database filters
3. Tag to be associated with the request

#### LargeData structure

LargeData is a single assembly containing code for both client as well server. 
Client directly references the assembly and uses LargeData.Client.LargeData class to send and recieve the datasets.
Server also references the assembly and registers the LargeData controller in the Global class (as mentioned in above section "Server Configuration")

![LargeData Structure](https://raw.githubusercontent.com/lokeshlal/LargeData/master/largedata_structure.png)

LargeData provides output in 2 format, 
1. Data set (generally for smaller set of data) 
2. Data reader (for large data sets)

REST APIs will have to provide a method to handle all incoming upload and download request. This will be a single method for all sort of upload and download request. So to handle various scenarios, this method also accept List&lt;Filter&gt; (as mentioned in section "Filter class") to differentiate between different type of request.

Please note, background worker process is running in ASP.NET context, this process can be triggered via Hangfire, Quartz or a separate windows service could also be created (based upon the requirements, in my original code, I have written a windows service for the background process, however I feel hangfire is a good fit).

#### Sample

Please have a look at the Client and WebApi project on how to use this library.
Client project contains code of how to trigger upload and download request.
WebApi project contains how to process upload and download request.
