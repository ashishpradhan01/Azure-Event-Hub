using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace EventConsumer
{
    public static class Function1
    {
        [FunctionName("Event_Consumer")]
        public static async Task Run([EventHubTrigger("multiple-part", Connection = "EVENTHUB_CONNECTION_STRING")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    // Replace these two lines with your processing logic.
                    log.LogInformation(Encoding.UTF8.GetString(eventData.Body));
                    log.LogInformation($"Sequence number {eventData.SystemProperties}");
                    log.LogInformation($"Sequence number {eventData.SystemProperties.SequenceNumber}");
                    log.LogInformation($"Offset {eventData.SystemProperties.Offset}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
