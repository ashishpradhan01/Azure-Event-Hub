

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
        //private static readonly string conn_string = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=Send;SharedAccessKey=UzcyTqjNMKUk2K0hUJKYdQICA3nuIM35epHSgCU/3rE=;EntityPath=single-part";

        //Multiple Partition - Event Hub
        private static readonly string conn_string = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=Common;SharedAccessKey=1tfFHq2kjsIAdbuZH+p6gRqTIeEUFsjChxcfDGljRck=;EntityPath=multiple-part";
        private static readonly bool multiple = true;
        static void Main(string[] args)
        {

            EventHubProducerClient hub_client = new EventHubProducerClient(conn_string);

            //EventDataBatch hub_batch = hub_client.CreateBatchAsync().GetAwaiter().GetResult();
            List<EventData> hub_batch = new List<EventData>();

            List<string> messages = new List<string>();

            for (int i = 900; i < 950; i++)
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
                options: new SendEventOptions { PartitionKey="Order" }
                ).GetAwaiter().GetResult();
            Console.WriteLine("!! All messages are sent !!");
        }
    }
}
