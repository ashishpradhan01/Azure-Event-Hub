using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using System.Threading.Tasks;
using System.Text;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace EventHubTimer
{
    public class MyEvent
    {
        public string Sequence { get; set; }
        public string Offset { get; set; }
        public string Partkey { get; set; }
        public string Body { get; set; }

        public MyEvent(string sq, string off, string key, string body)
        {
            Sequence = sq;
            Offset = off;
            Partkey = key;
            Body = body;
        }
        public override string ToString() =>  $"Sequence number: {Sequence}\nOffest: {Offset}\nPartitionKey: {Partkey}\n{Body}";
    }

    public class Function1
    {
        static List<MyEvent> eventList = new List<MyEvent>();
        //static DataTable dataTable = GetEventTable();
        static string sqlConnection = "Server=tcp:server-ashish.database.windows.net,1433;Initial Catalog=db-ashish;Persist Security Info=False;User ID=ash-admin;Password=$$12345P;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        static string tableName = "Messages";
        
        [FunctionName("EventHubTimer")]
        public async Task RunAsync([TimerTrigger("08 20 * * *")]TimerInfo myTimer, ILogger log)
        {
            
            log.LogInformation($"Starting Event Hub Receiver at: {DateTime.Now}");


            string hubNamespaceConnectionString = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=QTsQtcOM5SWhWxd5ArqS2N2LS9q2hqR0/w9X9I6/gek=";
            string eventHubName = "demohub";
            string consumerGroup = "$Default";

            string blobConnectionString = "DefaultEndpointsProtocol=https;AccountName=storashish;AccountKey=2Wyj3XXhQFlHGaXS1+y6N7wQHHrvjmE+mE4GoMrCFVCPDK1lgRb7I5eLz26y3HB/vV70X9wuSaVQ+AStHrJ4Iw==;EndpointSuffix=core.windows.net";
            string containerName = "offset";


            BlobContainerClient blobContainerClient = new BlobContainerClient(blobConnectionString, containerName);

            EventProcessorClient processor = new EventProcessorClient(blobContainerClient, consumerGroup, hubNamespaceConnectionString, eventHubName);

            //register
            processor.ProcessEventAsync += Processor_ProcessEventAsync;
            processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

            processor.StartProcessing();
            Console.WriteLine("Started the processor");

            Console.ReadLine();

            processor.StopProcessing();
            Console.WriteLine("Ended the processor");

            //unregister
            processor.ProcessEventAsync -= Processor_ProcessEventAsync;
            processor.ProcessErrorAsync -= Processor_ProcessErrorAsync;


            Console.WriteLine("Started Inserting in db");

            await InsertIntoDB2();
            




            /* if (dataTable.Rows.Count > 0)
             {
                 Console.WriteLine("Batch Insert started...");
                 log.LogInformation("Batch insert into the dedicated SQL pool.");
                 BatchInsert(dataTable, log, sqlConnection, tableName);
             }*/

            //Console.ReadLine();

            /*InsertIntoDB(sqlConnection, tableName);*/
        }

        private static Task Processor_ProcessErrorAsync(ProcessErrorEventArgs arg)
        {
            Console.WriteLine("Error Received: " + arg.Exception.ToString());
            return Task.CompletedTask;
        }

        private static async Task Processor_ProcessEventAsync(ProcessEventArgs eventArgs)
        {

            Console.WriteLine($"Sequence number: {eventArgs.Data.SequenceNumber}");
            Console.WriteLine($"Offest: {eventArgs.Data.Offset}");
            Console.WriteLine($"PartitionKey: {eventArgs.Data.PartitionKey}");
            Console.WriteLine(Encoding.UTF8.GetString(eventArgs.Data.EventBody));

            /* AddEventToTable(dataTable, new MyEvent(
                 eventArgs.Data.SequenceNumber.ToString(),
                 eventArgs.Data.Offset.ToString(),
                 eventArgs.Data.PartitionKey,
                 Encoding.UTF8.GetString(eventArgs.Data.EventBody)));*/
            Console.WriteLine("Inserted in list");
            eventList.Add(new MyEvent(
                eventArgs.Data.SequenceNumber.ToString(),
                eventArgs.Data.Offset.ToString(),
                eventArgs.Data.PartitionKey,
                Encoding.UTF8.GetString(eventArgs.Data.EventBody)));
            /*Console.WriteLine("Inserting Into Database.....");

            InsertIntoDB(tableName, new MyEvent(
                 eventArgs.Data.SequenceNumber.ToString(),
                 eventArgs.Data.Offset.ToString(),
                 eventArgs.Data.PartitionKey,
                 Encoding.UTF8.GetString(eventArgs.Data.EventBody)));*/

            Console.WriteLine("-----Check point updated-----");
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);  //To update the checkpoints in storage

        }

        public static void InsertIntoDB(string tableName, MyEvent item)
        {
            SqlConnection connection = new SqlConnection(sqlConnection);
            string query = $"INSERT INTO {tableName} (SequenceNumber,OffsetNumber,PartitionKey,Body) VALUES(@SequenceNumber, @OffsetNumber, @PartitionKey, @Body)";
            SqlCommand command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@SequenceNumber", item.Sequence);
            command.Parameters.AddWithValue("@OffsetNumber", item.Offset);
            command.Parameters.AddWithValue("@PartitionKey", item.Partkey);
            command.Parameters.AddWithValue("@Body", item.Body);

            try
            {
                connection.Open();
                command.ExecuteNonQuery();
                //command.ExecuteNonQueryAsync();

                Console.WriteLine(item.ToString());
               
                Console.WriteLine("Records Inserted Successfully");
            }
            catch (SqlException e)
            {
                Console.WriteLine("Error Generated. Details: " + e.ToString());
            }
            finally
            {
                connection.Close();
            }
        }




        public static async Task<Task> InsertIntoDB2()
        {
            SqlConnection connection = new SqlConnection(sqlConnection);
            try
            {
                connection.Open();
                foreach (var item in eventList)
                {
                    string query = $"INSERT INTO {tableName} (SequenceNumber,OffsetNumber,PartitionKey,Body) VALUES(@SequenceNumber, @OffsetNumber, @PartitionKey, @Body)";
                    SqlCommand command = new SqlCommand(query, connection);
                    command.Parameters.AddWithValue("@SequenceNumber", item.Sequence);
                    command.Parameters.AddWithValue("@OffsetNumber", item.Offset);
                    command.Parameters.AddWithValue("@PartitionKey", item.Partkey);
                    command.Parameters.AddWithValue("@Body", item.Body);
                    //command.ExecuteNonQuery();
                    await command.ExecuteNonQueryAsync();
                    Console.WriteLine(item.ToString());
                    Console.WriteLine("Records Inserted Successfully");
                }
                //command.ExecuteNonQueryAsync();
            }
            catch (SqlException e)
            {
                Console.WriteLine("Error Generated. Details: " + e.ToString());
            }
            finally
            {
                connection.Close();
            }

            return Task.CompletedTask;
        }


        /// For each parsed record, add a row to the in-memory table.
       /* private static void AddEventToTable(DataTable table, MyEvent mEvent)
        {
            table.Rows.Add(mEvent.Sequence, mEvent.Offset, mEvent.Partkey, mEvent.Body);
        }*/

        /// Define the in-memory table to store the data. The columns match the columns in the .sql script.
       /* private static DataTable GetEventTable()
        {
            var dt = new DataTable();
            dt.Columns.AddRange
            (
                new DataColumn[4]
                {
                    new DataColumn("SequenceNumber", typeof(string)),
                    new DataColumn("OffsetNumber", typeof(string)),
                    new DataColumn("PartitionKey", typeof(string)),
                    new DataColumn("Body", typeof(string))
                }
            );

            return dt;
        }*/

        /*private static void BatchInsert(DataTable table, ILogger log, string sqlConnection, string TableName)
        {
            // Write the data to SQL DW using SqlBulkCopy
            using (var sqlDwConnection = new SqlConnection(sqlConnection))
            {
                sqlDwConnection.Open();

                log.LogInformation("Bulk copying data.");
                using (var bulkCopy = new SqlBulkCopy(sqlConnection))
                {
                    bulkCopy.BulkCopyTimeout = 30;
                    bulkCopy.DestinationTableName = TableName;
                    bulkCopy.WriteToServer(table);
                }
            }
        }*/
    }
}
