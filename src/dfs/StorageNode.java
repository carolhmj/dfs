package dfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import static java.nio.file.StandardOpenOption.*;

//Classe que representa um nó de armazenamento
public class StorageNode implements DFS{
	public static final String queueName = "storage_queue_";
	// Id do nó de armazenamento, é o mesmo do mapa de réplicas
	String id;
	//Os arquivos serão criados em uma pasta em path
	Path path;
	
	public StorageNode(String id) throws IOException {
		this.id = id;
		String pathString = id;
		this.path = Paths.get(pathString);
		//Cria uma pasta com o id do nó de armazenamento
		Files.createDirectories(path);
	}
	
	public void start() throws Exception {
		String requestQueueName = queueName+id;
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(requestQueueName, false, false, false, null);

		channel.basicQos(1);

		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(requestQueueName, false, consumer);

		System.out.println(" [x] Awaiting RPC requests");

		while (true) {
		    QueueingConsumer.Delivery delivery = consumer.nextDelivery();

		    BasicProperties props = delivery.getProperties();
		    AMQP.BasicProperties replyProps = new AMQP.BasicProperties().builder().correlationId(props.getCorrelationId()).build();
		    String message = new String(delivery.getBody());
		    
		    //Parseia a mensagem, e cria a resposta
		    
		    String response = "";

		    channel.basicPublish( "", props.getReplyTo(), replyProps, response.getBytes());

		    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		}
		
		
	}
	
	public StorageNode(String id, String pathToDir) throws IOException {
		this.id = id;
		Path toDir = Paths.get(pathToDir);
		this.path = toDir.resolve(id);
		//Cria uma pasta com o id do nó de armazenamento
		Files.createDirectories(path);
	}
	
	public StorageNode() { }
	
	@Override
	public void create(String name, String content) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(path.resolve(name), CREATE, WRITE)) {
			writer.write(content);
			writer.close();
		} catch (IOException e) {
			throw e;
		}
	}

	@Override
	public String read(String name) throws IOException {
			try (BufferedReader reader = Files.newBufferedReader(path.resolve(name))) {
			String content = "";
			String line = "";
			while ((line = reader.readLine()) != null) {
				content = content + line;
				content = content + "\n";
			}
			reader.close();
			return content;
		} catch (IOException e) {
			throw e;
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: StorageNode [node id] [(OPTIONAL) path to file]");
		} else {
			String name = args[0];
			StorageNode node;
			
			if (args.length == 1) {
				node = new StorageNode(name);
			} else {
				String path = args[1];
				node = new StorageNode(name, path);
			}
			
			node.start();

		}
	}
}
