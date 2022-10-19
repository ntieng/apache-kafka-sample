using Confluent.Kafka;
using Newtonsoft.Json;

// Declare topic
string topic = "weather-topic";

// Declare producer config
// localhost:9092 is broker
var config = new ConsumerConfig
{
    GroupId = "weather-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

// Build new consumer
using var consumer = new ConsumerBuilder<Null, string>(config).Build();

// Subscribe to topic
consumer.Subscribe(topic);

CancellationTokenSource token = new();
try
{
    while (true)
    {
        var response = consumer.Consume(token.Token);
        if (response.Message != null)
        {
            // Receive message
            var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
            Console.WriteLine($"State: {weather.State}, " + $"Temp: {weather.Temperature}F");
        }
    }
}
catch (ConsumeException ex)
{
    Console.WriteLine(ex.Message);
}

public record Weather(string State, int Temperature);