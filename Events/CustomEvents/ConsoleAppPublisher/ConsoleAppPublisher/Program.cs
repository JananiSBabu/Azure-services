using System;
using System.Threading.Tasks;
using System.Xml;
using Azure;
using Azure.Messaging.EventGrid;

namespace ConsoleAppPublisher
{
    class Program
    {
        private const string topicEndpoint = "https://accountcreated.westus-1.eventgrid.azure.net/api/events";
        private const string topicKey = "VjRO7dnpP7BviNBVUtOpndlWZlxN13mNjnPc2gPkWkg=";

        private static readonly Uri uriEndpoint = new Uri(topicEndpoint);


        static async Task Main(string[] args)
        {
            // authenticate to event grid -> azure key credential
            AzureKeyCredential credential = new AzureKeyCredential(topicKey);

            EventGridPublisherClient client = new EventGridPublisherClient(uriEndpoint, credential);

            EventGridEvent acct = new EventGridEvent(
                subject: "New account",
                eventType: "New account created",
                dataVersion: "1.0",
                data: new { Message = "Hi" });

            await client.SendEventAsync(acct);
            Console.WriteLine("Event published");

            Console.ReadKey();
        }
    }
}
