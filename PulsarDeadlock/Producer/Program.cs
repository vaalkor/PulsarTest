using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Producer;
using Pulsar.Client.Api;
using System.Text;

const int NUM_RECORDS = 2000000;


var loggerFactory =
LoggerFactory.Create(builder =>
{
    builder
    .SetMinimumLevel(LogLevel.Warning)
    .AddConsole();
});

PulsarClient.Logger = loggerFactory.CreateLogger("PulsarLogger");

IProducer<byte[]> _producer;

var client = await new PulsarClientBuilder()
    .OperationTimeout(TimeSpan.FromSeconds(5))
    .ServiceUrl(Constants.PULSAR_URL)
    .BuildAsync();

_producer = await client.NewProducer()
    .Topic(Constants.TOPIC_NAME)
    .CompressionType(Pulsar.Client.Common.CompressionType.None)
    .EnableBatching(false)
    .BlockIfQueueFull(true)
    .SendTimeout(TimeSpan.FromSeconds(0))
    .CreateAsync();

int count = 0;
DateTime start = DateTime.Now;

var newMessage = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new LargeMessage()));
for (var i = 0; i < NUM_RECORDS; i++)
{
    await _producer.SendAndForgetAsync(newMessage);
    if (++count % 100 == 0)
    {
        Console.WriteLine($"{(DateTime.Now - start).TotalSeconds}s: Sent {count} messages");
    }    
}

public class LargeMessage
{
    private static readonly int SIZE = 5000;
    public LargeMessage()
    {
        Random rnd = new Random();

        for (int j = 0; j < SIZE; j++)
        {
            data[j] = rnd.Next();
        }
    }

    public int[] data = new int[SIZE];
}