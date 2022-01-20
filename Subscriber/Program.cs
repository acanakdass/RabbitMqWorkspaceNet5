using System;
using System.IO;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Subscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            //subscribeFanoutExchange();
            subscribTopicExchange();
        }


        private static void subscribTopicExchange()
        {
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            //channel.BasicQos(0, 1, false);
            //var criticalQueueName = "direct-queue-Critical";
            var queueName = channel.QueueDeclare().QueueName;

            var routeKey = "*.Warning.*";

            var consumer = new EventingBasicConsumer(channel);
            channel.QueueBind(queueName, "logs-topic", routeKey);

            channel.BasicConsume(queueName, false, consumer);

            Console.WriteLine($"Listening queue: {queueName}...");

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                Thread.Sleep(200);
                var message = Encoding.UTF8.GetString(e.Body.ToArray());
                Console.WriteLine("Gelen Mesaj:" + message);

                //File.AppendAllText("log-warning.txt", message + "\n");

                channel.BasicAck(e.DeliveryTag, true);
            };


            Console.ReadLine();
        }

        private static void subscribeDirectExchange()
        {
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            //channel.BasicQos(0, 1, false);
            //var criticalQueueName = "direct-queue-Critical";
            var warningQueueName = "direct-queue-Warning";

            var consumer = new EventingBasicConsumer(channel);

            channel.BasicConsume(warningQueueName,false,consumer);

            Console.WriteLine($"Listening queue: {warningQueueName}...");

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                Thread.Sleep(200);
                var message = Encoding.UTF8.GetString(e.Body.ToArray());
                Console.WriteLine("Gelen Mesaj:" + message);

                //File.AppendAllText("log-warning.txt", message + "\n");

                channel.BasicAck(e.DeliveryTag,true);
            };


            Console.ReadLine();
        }

        private static void subscribeFanoutExchange()
        {
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            var randomQueueName = channel.QueueDeclare().QueueName;


            channel.QueueBind(randomQueueName, "logs-fanout", "", null);


            channel.BasicQos(0, 1, false);

            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(randomQueueName, false, consumer);

            Console.WriteLine($"Listening queue: {randomQueueName}...");

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                Thread.Sleep(100);
                var message = Encoding.UTF8.GetString(e.Body.ToArray());
                Console.WriteLine("Gelen Mesaj:" + message);
                channel.BasicAck(e.DeliveryTag, false);
            };


            Console.ReadLine();
        }



        private static void subscribeQueue()
        {
            var factory = new ConnectionFactory();
            factory.HostName = "localhost";
            using var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.BasicQos(0, 1, false);


            //durable: false ise oluşan kuyruklar memory'de tutulur, rabbit mq restart olduğunda kuyruklar silinir
            //true ise fiziksel olarak tutulur
            //exclusive: false ise başka kuyruklardan bağlanılabilir
            //channel.QueueDeclare("hello-queue", durable: true, exclusive: false, autoDelete: false);


            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume("hello-queue", false, consumer);

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                Thread.Sleep(100);
                var message = Encoding.UTF8.GetString(e.Body.ToArray());
                Console.WriteLine("Gelen Mesaj:" + message);
                channel.BasicAck(e.DeliveryTag, false);
            };


            Console.ReadLine();
        }
    }
}
