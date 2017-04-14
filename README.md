# LargeData

Recently I was working with a problem, where we have to transfer 5 GB (maximum or first load) of data set across client (WPF client) and server (Web APIs) (both download and upload) (don't ask me why, but yes that was the requirement). All this need to happen over vpn connection and users are across the world with a varying internet speed (which sometimes could be as low as 16 kbps).

Initial implementation was to leverage the existing sync frameworks to synchronize the data and let the framework handle all the changes done by the user in the database. However, the sync framework was making things very slow because of database triggers (impacting inserts and updates, as in one transaction a maximum of 100K records could be inserted or updated)

Time taken by sync framework was approximately ~30 minutes to sync the entire data from server to client and  >1 hr to sync back from client to server.

To solve this performance bottleneck, first thing is to get rid of sync framework. but then again how do we keep track of changes without much changes in existing code (as everything else is working just fine except the data synchronization)

This part is not covered in the above library, that is, how to keep track of what changed.
So we already have a "rowversion" column in all the tables (we were using SQL server), which is always incremental across the DB. So to keep track of childs, what we need to do is to add a LastSyncedOn column in the parent table (for all the child table) and set LastSyncedOn to @DBTS. which means last time database was synced on that this timestamp and if any child table rowversion is greater than this value, then it a new row (either inserted or updated).

On the basis of this we created the DataSet, both at client and server. Feed the dataset to the above utility and send it across.

At some places, we also ended up using datareader, as dataset was giving out of memory exception (in the current code, I have only used dataset, howover that can be replaced with the datareader as well)

Note: still working on this library. Adding client to server upload, which requires splitting/stiching of files. Also working on adding data reader support, otherwise in some scenarios (huge dataset) the code will throw outofmemory exception.
