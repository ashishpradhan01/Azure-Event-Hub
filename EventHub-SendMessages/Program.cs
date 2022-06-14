

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventHub_SendMessages
{
    class Program
    {
        //Single Partition - Event Hub
        private static readonly string conn_string = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=QTsQtcOM5SWhWxd5ArqS2N2LS9q2hqR0/w9X9I6/gek=";

        //Multiple Partition - Event Hub
        //private static readonly string conn_string = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=Common;SharedAccessKey=1tfFHq2kjsIAdbuZH+p6gRqTIeEUFsjChxcfDGljRck=;EntityPath=multiple-part";
        //private static readonly bool multiple = true;
        static void Main(string[] args)
        {

            EventHubProducerClient hub_client = new EventHubProducerClient(conn_string, "demohub");

            //EventDataBatch hub_batch = hub_client.CreateBatchAsync().GetAwaiter().GetResult();
            List<EventData> hub_batch = new List<EventData>();

            List<string> messages = new List<string>();

            for (int i = 10; i <= 20; i++)
            {
               /* if(multiple) messages.Add($"Hello message from Multiple Partition - {i}");
                else messages.Add($"Hello message from Single Partition - {i}");
*/

                hub_batch.Add(new EventData($"Message:: {i}"));
            }
        

            /*foreach (string mgs in messages)
            {
                hub_batch.TryAdd(new Azure.Messaging.EventHubs.EventData(Encoding.UTF8.GetBytes(mgs)));
                Console.WriteLine($"This [ {mgs} ] is sent successfully");
            }*/

            //hub_client.SendAsync(hub_batch).GetAwaiter().GetResult();

            hub_client.SendAsync(
                eventBatch: hub_batch,
                options: new SendEventOptions { PartitionKey= "dblogstream" }
                ).GetAwaiter().GetResult();
            Console.WriteLine("!! All messages are sent !!");
        }
    }
}
