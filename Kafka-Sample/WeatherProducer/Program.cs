using Confluent.Kafka;
using Newtonsoft.Json;

// Declare topic
string topic = "weather-topic";

// Declare producer config
// localhost:9092 is broker
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

// Build new producer
using var producer = new ProducerBuilder<Null, string>(config).Build();

try
{
    string? state;
    while ((state = Console.ReadLine()) != null)
    {
        // Send message
        var response = await producer.ProduceAsync(topic, new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(state, 70)) });
        Console.WriteLine(response.Value);
    }
}
catch (ProduceException<Null, string> ex)
{
    Console.WriteLine(ex.Message);
}

public record Weather(string State, int Temperature);