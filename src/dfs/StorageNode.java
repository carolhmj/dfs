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
	public static final String queueBasicName = "storage_queue";
	public static final String messageSeparator = ",";
	//Queue name with id
	public String queueTrueName;
	// Id do nó de armazenamento, é o mesmo do mapa de réplicas
	String id;
	//Os arquivos serão criados em uma pasta em path
	Path path;
	
	public StorageNode(String id) throws IOException {
		this.id = id;
		this.queueTrueName = queueBasicName+"."+id;
		String pathString = id;
		this.path = Paths.get(pathString);
		//Cria uma pasta com o id do nó de armazenamento
		Files.createDirectories(path);
	}
	
	public StorageNode(String id, String pathToDir) throws IOException {
		this.id = id;
		this.queueTrueName = queueBasicName+"."+id;
		Path toDir = Paths.get(pathToDir);
		this.path = toDir.resolve(id);
		//Cria uma pasta com o id do nó de armazenamento
		Files.createDirectories(path);
	}
	
	public StorageNode() { }

	
	public void start() throws Exception {
	
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		channel.exchangeDeclare(exchangeName, "topic");

		channel.queueDeclare(queueTrueName, false, false, false, null);
		channel.queueBind(queueTrueName, exchangeName, queueTrueName);
		
		channel.basicQos(1);

		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueTrueName, false, consumer);

		System.out.println("Awaiting requests on [" + queueTrueName + "]");

		while (true) {
		    QueueingConsumer.Delivery delivery = consumer.nextDelivery();

		    BasicProperties props = delivery.getProperties();
		    AMQP.BasicProperties replyProps = new AMQP.BasicProperties().builder().correlationId(props.getCorrelationId()).build();
		    String message = new String(delivery.getBody());
		    String response = "";
		    
		    System.out.println("Received message [" + message + "]");
		    
		    //Parseia a mensagem, e cria a resposta
		    String[] messageArgs = message.split(messageSeparator);
		    //Criar um arquivo, retorna verdadeiro ou falso
		    if (messageArgs[0].equals("CREATE")) {
		    	String arqName = messageArgs[1];
		    	String content = messageArgs[2];
		    	boolean opResponse = create(arqName, content);
		    	response += Boolean.toString(opResponse);
		    }
		    /*
		     * Ler um arquivo, retorna o conteúdo do arquivo ou ERROR caso
		     * não tenha sido possível ler 
		     */
		    else if (messageArgs[0].equals("READ")) {
		    	String arqName = messageArgs[1];
		    	String readContent = read(arqName);
		    	if (readContent == null) {
		    		response += "ERROR";
		    	} else {
		    		response += readContent;
		    	}
		    } 
		    //Mensagem inválida, retorna ERROR
		    else {
		    	response += "ERROR";
		    }
		    
		    System.out.println("Sending response [" + response + "]");
		    
		    channel.basicPublish( "", props.getReplyTo(), replyProps, response.getBytes());

		    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		}
		
		
	}
		
	@Override
	public boolean create(String name, String content) {
		try (BufferedWriter writer = Files.newBufferedWriter(path.resolve(name), CREATE, WRITE)) {
			writer.write(content);
			writer.close();
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public String read(String name) {
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
			return null;
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
