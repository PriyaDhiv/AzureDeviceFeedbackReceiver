using Microsoft.Azure.Devices;
using Microsoft.Azure.WebJobs;
using NextGenGw.WebJobs.Models;
using NextGenGw.WebJobs.Services.DeviceFeedback;
using NextGenGw.WebJobs.StorageService;
using NextGenGw.WebJobs.Utils;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FeedbackReceiver
{
    public class Functions
    {
        // This function will get triggered/executed when a new message is written 
        // on an Azure Queue called queue.
        public static void ProcessTimerAction([TimerTrigger("*/1 * * * * *", RunOnStartup = true)] TimerInfo log)
        {
           // IFeedbackReceiver feedbackReceiver = new NextGenGw.WebJobs.Services.DeviceFeedback.FeedbackReceiver(ConfigurationManager.ConnectionStrings["IoTHubConnectionString"].ToString(),new Helper(ConfigurationManager.AppSettings["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString()), new TableStorageService());

            ReceiveFeedbackAsync(ConfigurationManager.ConnectionStrings["PrintupConnectionString"].ToString()).Wait();
        }
        public static async Task ReceiveFeedbackAsync(string terraConnString)
        {
            IHelper _helper = new Helper(ConfigurationManager.AppSettings["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString());
            ITableStorageService _cloudTableManager = new TableStorageService();
            ServiceClient serviceClient = ServiceClient.CreateFromConnectionString(ConfigurationManager.ConnectionStrings["IoTHubConnectionString"].ToString());
            var feedbackReceiver = serviceClient.GetFeedbackReceiver();

            DateTime time = DateTime.Now;
            string feedback = String.Empty;
            try
            {
                while (true)
                {
                    var feedbackBatch = await feedbackReceiver.ReceiveAsync();
                    if (feedbackBatch != null)
                    {
                        _helper.WriteInformation("Inside while loop " + DateTime.Now);
                        foreach (FeedbackRecord feedbackBatchRecord in feedbackBatch.Records)
                        {
                            string temp = $"Device Id:{feedbackBatchRecord.DeviceId}-Status:{feedbackBatchRecord.StatusCode} - Message Id :{feedbackBatchRecord.OriginalMessageId}";

                            feedback += $"|{temp}|";

                            var messageToTerra =
                                 new
                                 {
                                     statusCode = (int)feedbackBatchRecord.StatusCode,
                                     description = feedbackBatchRecord.Description
                                 };
                            // var eventData = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(messageToTerra)));

                            IDictionary<string, string> appValues = new Dictionary<string, string>();
                            appValues.Add("DeviceId", feedbackBatchRecord.DeviceId);
                            appValues.Add("DeviceGeneratedId", feedbackBatchRecord.DeviceGenerationId);
                            appValues.Add("MessageId", feedbackBatchRecord.OriginalMessageId);
                            appValues.Add("EnqueuedTimeUtc", feedbackBatchRecord.EnqueuedTimeUtc.ToString());

                            //eventData.Properties.Add("DeviceId", feedbackBatchRecord.DeviceId);
                            //eventData.Properties.Add("DeviceGeneratedId", feedbackBatchRecord.DeviceGenerationId);
                            //eventData.Properties.Add("MessageId", feedbackBatchRecord.OriginalMessageId);
                            //eventData.Properties.Add("EnqueuedTimeUtc", feedbackBatchRecord.EnqueuedTimeUtc.ToString());

                            //Sending to Terra
                            //  var hubClient = EventHubClient.CreateFromConnectionString(terraConnString);
                            // await hubClient.SendAsync(eventData);


                            // store in table
                            await _cloudTableManager.Insert<WOADeviceFeedbackEntity>("DeviceFeedbacks", feedbackBatchRecord.DeviceId, feedbackBatchRecord.OriginalMessageId, new WOADeviceFeedbackEntity() { DeviceStatus = feedbackBatchRecord.Description });

                        }
                        await feedbackReceiver.CompleteAsync(feedbackBatch);

                    }
                }
            }
            catch (Exception ex)
            {
                _helper.SendException(ex);
                _helper.WriteInformation("Feedback process unsuccessful..");
            }
        }
    }
}
