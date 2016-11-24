package dfs;

import java.io.IOException;
import java.util.Scanner;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

/*
 * Classe que representa um cliente. ele pede um id para o balanceador e com o id
 * manda solicitações para o proxy
 */

public class Client implements DFS {
	public static final String queueName = "client_queue"; 
	
	//RPC
	Connection connection;
	Channel channel;
	String replyQueueName;
	QueueingConsumer consumer;
	String ip = "localhost";
	
	public Client(String ip) {
		this.ip = ip;
	}
	
	public Client() {
	}

	public void start() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(ip);
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    channel.exchangeDeclare(DFS.exchangeName, "topic");

	    replyQueueName = channel.queueDeclare().getQueue();
	    channel.queueBind(replyQueueName, DFS.exchangeName, queueName);
	    
	    consumer = new QueueingConsumer(channel);
	    channel.basicConsume(replyQueueName, true, consumer);
	    
		channel.basicQos(1);

	}
	
	@Override
	public boolean create(String name, String content) {
		//Pergunta um ID pro nó balanceador
		String proxyId = getProxyId();
		String proxyQueue = ProxyNode.queueBasicName + "." + proxyId;
		
		String response = null;
	    String corrId = java.util.UUID.randomUUID().toString();

	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(replyQueueName)
	                                .build();

	    //Making the message
	    String message = "";
	    
	    message = message + "CREATE" + "," + name + "," + content;
	    	    
	    try {
	    	channel.basicPublish(DFS.exchangeName, proxyQueue, props, message.getBytes());
			System.out.println("Sent request for [" + proxyQueue + "] Awaiting response...");
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

	    //Parse the response
		if (response.equals("ERROR")) { return false; }
		return Boolean.parseBoolean(response);
	}

	@Override
	public String read(String name) {
		//Pergunta um ID pro nó balanceador
		String proxyId = getProxyId();
		String proxyQueue = ProxyNode.queueBasicName + "." + proxyId;
		
		String response = null;
	    String corrId = java.util.UUID.randomUUID().toString();

	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(replyQueueName)
	                                .build();

	    //Making the message
	    String message = "";
	    
	    message = message + "READ" + "," + name;
	    	    
	    try {
	    	channel.basicPublish(DFS.exchangeName, proxyQueue, props, message.getBytes());
			System.out.println("Sent request for [" + proxyQueue + "] Awaiting response...");
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

		return response;
	}
	
	//Função pra fazer a solicitação de um id de proxy
	public String getProxyId() {
		String balanceQueueName = BalanceNode.queueName;
		
		String response = null;
	    String corrId = java.util.UUID.randomUUID().toString();

	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(replyQueueName)
	                                .build();

	    //Making the message
	    String message = "";
	    
	    message = message + "ID";
	    	    
	    try {
	    	channel.basicPublish(DFS.exchangeName, balanceQueueName, props, message.getBytes());
			System.out.println("Sent request for [" + balanceQueueName + "] Awaiting response...");
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

	    System.out.println("Received response [" + response + "]");
	    return response;
	}

	public static void main(String[] args) throws Exception {
		Scanner scanner = new Scanner(System.in);
		boolean finished = false;
		
		Client client;
		if (args.length == 0) {
			client = new Client();
		} else {
			client = new Client(args[0]);
		}
		client.start();
		
		while (!finished) {
			System.out.println("Options:");
			System.out.println("(C)reate");
			System.out.println("(R)ead");
			System.out.println("(Q)uit");
			
			String option = scanner.nextLine();
			
			switch (option) {
			case "C":
			case "c":
				System.out.println("Insert the archive name:");
				String name = scanner.nextLine();
				System.out.println("Insert the archive contents:");
				String content = scanner.nextLine();
				String result = client.create(name, content)? "success" : "fail";
				System.out.println("The creation of " + name + " was a " + result);
				break;
			case "R":
			case "r":
				System.out.println("Insert the archive name:");
				String nameR = scanner.nextLine();
				System.out.println("The contents of the archive are: " + client.read(nameR));
				break;
			case "Q":
			case "q":
				finished = true;
				break;
			default:
				System.out.println("Incorrect option");
				break;
			}
		}
		
		scanner.close();
	}
}
