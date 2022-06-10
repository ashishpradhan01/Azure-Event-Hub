using Azure.Storage.Blobs;
using System;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using System.Threading.Tasks;
using System.Text;

namespace EventHubOffReceiver
{
    class Program
    {
		static async Task Main(string[] args)
		{
			Console.WriteLine("Starting our Event Hub Receiver");



			string hubNamespaceConnectionString = "Endpoint=sb://hubnamespace-ashish.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=QTsQtcOM5SWhWxd5ArqS2N2LS9q2hqR0/w9X9I6/gek=";
			string eventHubName = "single-part";


			string blobConnectionString = "DefaultEndpointsProtocol=https;AccountName=storashish;AccountKey=2Wyj3XXhQFlHGaXS1+y6N7wQHHrvjmE+mE4GoMrCFVCPDK1lgRb7I5eLz26y3HB/vV70X9wuSaVQ+AStHrJ4Iw==;EndpointSuffix=core.windows.net";
			string containerName = "eventhub-offsetcontainer";


			BlobContainerClient blobContainerClient = new BlobContainerClient(blobConnectionString, containerName);

            EventProcessorClient processor = new EventProcessorClient(blobContainerClient, "$Default", hubNamespaceConnectionString, eventHubName);
           

            processor.ProcessEventAsync += Processor_ProcessEventAsync;
            processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

            await processor.StartProcessingAsync();
            Console.WriteLine("Started the processor");


            Console.ReadLine();
            await processor.StopProcessingAsync();
            Console.WriteLine("Ended the processor");

        }

        private static Task Processor_ProcessErrorAsync(ProcessErrorEventArgs arg)
        {
            Console.WriteLine("Error Received: " + arg.Exception.ToString());
            return Task.CompletedTask;
        }

        private static async Task Processor_ProcessEventAsync(ProcessEventArgs eventArgs)
        {
            Console.WriteLine($"Sequence number {eventArgs.Data.SequenceNumber}");
            Console.WriteLine(Encoding.UTF8.GetString(eventArgs.Data.EventBody));
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);  //To Update the checkpoints in storage

        }
    }
}
