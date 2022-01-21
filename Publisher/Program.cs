using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace Publisher
{
    class Program
    {
        static void Main(string[] args)
        {


            //FanoutExchange();
            //TopicExchange();
            HeaderExchange();


        }

        enum LogNames
        {
            Critical = 1,
            Error = 2,
            Warning = 3,
            Info = 4
        }


        static void HeaderExchange()
        {
            //HEADER EXCHANGE//
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.ExchangeDeclare("header-exchange", durable: true, type: ExchangeType.Headers);

            Dictionary<string, object> headers = new Dictionary<string, object>();

            headers.Add("format", "pdf");
            headers.Add("shape", "a4");

            var properties = channel.CreateBasicProperties();
            properties.Headers = headers;

            properties.Persistent = true;

            var msg = Encoding.UTF8.GetBytes("Header message");
            channel.BasicPublish("header-exchange", string.Empty, properties, msg);

            Console.WriteLine("Message Sent");


            Console.ReadLine();
        }


        static void TopicExchange()
        {
            //TOPIC EXCHANGE//
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.ExchangeDeclare("logs-topic", durable: true, type: ExchangeType.Topic);

            var rnd = new Random();
            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {

                var randomNum = new Random().Next(1, 5);
                var log = (LogNames)randomNum;

                LogNames log1 = (LogNames)rnd.Next(1, 5);
                LogNames log2 = (LogNames)rnd.Next(1, 5);
                LogNames log3 = (LogNames)rnd.Next(1, 5);

                var routeKey = $"{log1}.{log2}.{log3}";
                string message = $"{x})log-type: {log1}-{log2}-{log3}";
                var messageBody = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs-topic", routeKey, null, messageBody);
                Console.WriteLine($"Log {x} Sent:{message}");
            });

            Console.ReadLine();
        }

        static void DirectExchange()
        {
            //DIRECT EXCHANGE//
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.ExchangeDeclare("logs-direct", durable: true, type: ExchangeType.Direct);

            Enum.GetNames(typeof(LogNames)).ToList().ForEach(x =>
            {
                var routeKey = $"route-{x}";
                var queueName = $"direct-queue-{x}";
                channel.QueueDeclare(queueName, true, false, false);
                channel.QueueBind(queueName, "logs-direct", routeKey, null);
            });

            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {

                var randomNum = new Random().Next(1, 5);
                var log = (LogNames)randomNum;


                string message = $"{x})log-type {log}";
                var messageBody = Encoding.UTF8.GetBytes(message);

                var routeKey = $"route-{log}";

                channel.BasicPublish("logs-direct", routeKey, null, messageBody);
                Console.WriteLine($"Log {x} Sent. log-type: {log}");
            });

            Console.ReadLine();
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

