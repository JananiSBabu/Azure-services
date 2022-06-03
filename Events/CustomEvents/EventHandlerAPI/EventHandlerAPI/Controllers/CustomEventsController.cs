using System;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Threading.Tasks;
using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventHandlerAPI.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class CustomEventsController : ControllerBase
    {
        private readonly ILogger<CustomEventsController> _logger;

        public CustomEventsController(ILogger<CustomEventsController> logger)
        {
            _logger = logger;
        }
        
        [HttpPost]
        public async Task<IActionResult> Post()
        {
            string response = string.Empty;
            BinaryData events = await BinaryData.FromStreamAsync(Request.Body);


            EventGridEvent[] eventGridEvents = EventGridEvent.ParseMany(events);

            foreach (EventGridEvent eventGridEvent in eventGridEvents)
            {
                // Handle system events
                if (eventGridEvent.TryGetSystemEventData(out object eventData))
                {
                    // Handle the subscription validation event
                    if (eventData is SubscriptionValidationEventData subscriptionValidationEventData)
                    {
                        // Log.LogInformation($"Got SubscriptionValidation event data, validation code: {subscriptionValidationEventData.ValidationCode}, topic: {eventGridEvent.Topic}");
                        // Do any additional validation (as required) and then return back the below response

                        var responseData = new SubscriptionValidationResponse()
                        {
                            ValidationResponse = subscriptionValidationEventData.ValidationCode
                        };
                        return new OkObjectResult(responseData);
                    }
                }
            }
            return new OkObjectResult(response);

        }
    }
}
