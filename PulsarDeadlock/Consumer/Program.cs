using Microsoft.Extensions.Logging;
using Producer;
using Pulsar.Client.Api;
using Pulsar.Client.Common;

var loggerFactory =
LoggerFactory.Create(builder =>
{
    builder
    .SetMinimumLevel(LogLevel.Information)
    .AddConsole();
});

PulsarClient.Logger = loggerFactory.CreateLogger("PulsarLogger");

var client = await new PulsarClientBuilder()
    .OperationTimeout(TimeSpan.FromSeconds(5))
    .ServiceUrl(Constants.PULSAR_URL)
    .BuildAsync();

var subName = Guid.NewGuid().ToString();
Console.WriteLine($"Subscribing to topic:{Constants.TOPIC_NAME} with subscription name:{subName}");

IConsumer<byte[]> _consumer = await client.NewConsumer()
    .Topic(Constants.TOPIC_NAME)
    .SubscriptionName(subName)
    .SubscriptionType(SubscriptionType.KeyShared)
    .ReceiverQueueSize(10000)
    .EnableRetry(true)
    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(3))
    .AckTimeout(TimeSpan.FromSeconds(2))
    .DeadLetterPolicy(new DeadLetterPolicy(2))
    .SubscribeAsync();

int count = 0;
DateTime start = DateTime.Now;

await Task.Run(async () =>
{
    while (true)
    {
        var message = await _consumer.ReceiveAsync();

        if (++count % 100 == 0)
        {
            Console.WriteLine($"{(DateTime.Now - start).TotalSeconds}s: Received {count} messages");
        }
        await _consumer.AcknowledgeAsync(message.MessageId);
    }
});
