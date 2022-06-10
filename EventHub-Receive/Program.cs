using Azure.Messaging.EventHubs.Consumer;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHub_Receive
{
    class Program
    {
        //Single Partition
        //private static readonly string conn_string = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=Receive;SharedAccessKey=vXSEoViewcFtyu4mrOTh2FHbsAjAoD9m04v2zJ6Tyl8=;EntityPath=single-part";

        //Multple Partition
        //private static readonly string conn_string = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=Common;SharedAccessKey=1tfFHq2kjsIAdbuZH+p6gRqTIeEUFsjChxcfDGljRck=;EntityPath=multiple-part";

        //SQL Logs
        private static readonly string conn_string = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=DBLOG;SharedAccessKey=qEZl27YbQkpsd9XHYP3gmAW+Rj7OMqGkffqKds94eng=;EntityPath=dblogstream";

        private static readonly string consumer_group = "$Default";
        static async Task Main(string[] args)
        {

            EventHubConsumerClient hub_client = new EventHubConsumerClient(consumer_group, conn_string);

            Console.WriteLine("Consuming events");

            await foreach (PartitionEvent _event in hub_client.ReadEventsAsync())
            {
                Console.WriteLine("***********************************************");
                Console.WriteLine($"Partition ID {_event.Partition.PartitionId}");
                Console.WriteLine($"Data Offset {_event.Data.Offset}");
                Console.WriteLine($"Sequence Number {_event.Data.SequenceNumber}");
                Console.WriteLine($"Partition Key {_event.Data.PartitionKey}");
                Console.WriteLine(Encoding.UTF8.GetString(_event.Data.EventBody));
                Console.WriteLine("***********************************************\n");
            }

            //From specific parition
            /*string PartitionId = (await hub_client.GetPartitionIdsAsync()).First();
            await foreach (PartitionEvent _event in hub_client.ReadEventsFromPartitionAsync(PartitionId, EventPosition.Earliest))
            {
                Console.WriteLine("***********************************************");
                Console.WriteLine($"Partition ID {_event.Partition.PartitionId}");
                Console.WriteLine($"Data Offset {_event.Data.Offset}");
                Console.WriteLine($"Sequence Number {_event.Data.SequenceNumber}");
                Console.WriteLine($"Partition Key {_event.Data.PartitionKey}");
                Console.WriteLine(Encoding.UTF8.GetString(_event.Data.EventBody));
                Console.WriteLine("***********************************************");
            }*/

            Console.ReadKey();
        }
    }
}
