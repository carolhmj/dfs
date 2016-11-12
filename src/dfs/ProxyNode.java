package dfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Vector;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;


//Classe que representa um nó Proxy
public class ProxyNode implements DFS {
	//Mapa de réplicas
	
	public ProxyNode(String pathToMapFileString) throws IOException {
		parseMapFile(pathToMapFileString);
	}
	
	public void start() throws Exception {
		Connection connection;
		Channel channel;
		String requestQueueName = "rpc_queue";
		String replyQueueName;
		QueueingConsumer consumer;
		
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
	public void create(String name, String content) throws IOException {

	}

	@Override
	public String read(String name) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static void main(String args[]) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: ProxyNode [path to map archive]");
		} else {
			ProxyNode proxyNode;
			proxyNode = new ProxyNode(args[0]);
			proxyNode.start();
		}
	}

}
