package dfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Vector;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


//Classe que representa um nó Proxy
public class ProxyNode implements DFS {
	public static final String queueName = "proxy_queue_";
	public static final String messageSeparator = ",";
	//Mapa de réplicas
	
	
	//RPC
	Connection connection;
	Channel channel;
	String requestQueueName = queueName + "request";
	String replyQueueName = queueName + "reply";
	String storageRequestQueueName = StorageNode.queueName;
	QueueingConsumer consumer;

	public ProxyNode(String pathToMapFileString) throws IOException {
//		parseMapFile(pathToMapFileString);
	}
	
	public void start() throws Exception {		
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    connection = factory.newConnection();
	    channel = connection.createChannel();

	    replyQueueName = channel.queueDeclare().getQueue(); 
	    consumer = new QueueingConsumer(channel);
	    channel.basicConsume(replyQueueName, true, consumer);
	}
	
	private void parseMapFile(String pathToMapFileString) throws IOException {
		Path pathToMapFile = Paths.get(pathToMapFileString);
		try (BufferedReader reader = Files.newBufferedReader(pathToMapFile)) {
			
		} catch (IOException e) {
			
		}
	}
	
	@Override
	public boolean create(String name, String content) {
		String response = null;
	    String corrId = java.util.UUID.randomUUID().toString();

	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(replyQueueName)
	                                .build();

	    //Making the message
	    String message = "";
	    
	    message = message + "CREATE" + messageSeparator + name + messageSeparator + content;
	    
	    try {
			channel.basicPublish("", storageRequestQueueName + "1", props, message.getBytes());
			System.out.println("Sent request for [" + storageRequestQueueName + "1" + "] Awaiting response...");
	    } catch (IOException e1) {
			e1.printStackTrace();
			response = new Boolean(false).toString();
		}

	    while (true) {
	        QueueingConsumer.Delivery delivery;
			try {
				delivery = consumer.nextDelivery();
		        if (delivery.getProperties().getCorrelationId().equals(corrId)) {
		            response = new String(delivery.getBody());
		            break;
		        }
		        
			} catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
				e.printStackTrace();
				response = new Boolean(false).toString();
			}
	    }

	    return Boolean.parseBoolean(response);

	}

	@Override
	public String read(String name) {
		String response = null;
	    String corrId = java.util.UUID.randomUUID().toString();

	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(replyQueueName)
	                                .build();

	    //Making the message
	    String message = "";
	    
	    message = message + "READ" + messageSeparator + name;
	    
	    try {
			channel.basicPublish("", storageRequestQueueName + "1", props, message.getBytes());
		} catch (IOException e1) {
			e1.printStackTrace();
			response = "ERROR";
		}

	    while (true) {
	        QueueingConsumer.Delivery delivery;
			try {
				delivery = consumer.nextDelivery();
		        if (delivery.getProperties().getCorrelationId().equals(corrId)) {
		            response = new String(delivery.getBody());
		            break;
		        }
		        
			} catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
				e.printStackTrace();
				response = "ERROR";
			}
	    }

	    if (response.equals("ERROR")) {
	    	return null;
	    }
	    return response;

	}
	
	public static void main(String args[]) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: ProxyNode [path to map archive]");
		} else {
			ProxyNode proxyNode;
			proxyNode = new ProxyNode(args[0]);
			proxyNode.start();
			proxyNode.create("lala.txt", "lalala");
			proxyNode.read("lala.txt");
		}
	}

}
