using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace Publisher
{
    class Program
    {
        static void Main(string[] args)
        {


            FanoutExchange();



        }


        static void FanoutExchange()
        {
            //FANOUT EXCHANGE//
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.ExchangeDeclare("logs-fanout", durable: true, type: ExchangeType.Fanout);


            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                string message = $"Log {x}";
                var messageBody = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish("logs-fanout", "", null, messageBody);
                Console.WriteLine($"Log {x} Sent");
            });

            Console.ReadLine();
        }



        private static void ToTheQueue()
        {
            //Directly to the queue// 

            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            //durable: false ise oluşan kuyruklar memory'de tutulur, rabbit mq restart olduğunda kuyruklar silinir
            //true ise fiziksel olarak tutulur
            //exclusive: false ise başka kuyruklardan bağlanılabilir
            channel.QueueDeclare("hello-queue", durable: true, exclusive: false, autoDelete: false);


            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                string message = $"Message {x}";
                var messageBody = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(string.Empty, "hello-queue", null, messageBody);
                Console.WriteLine($"Message {x} Sent");
            });

            Console.ReadLine();
        }
    }

}

