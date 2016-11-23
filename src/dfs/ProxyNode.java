package dfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


//Classe que representa um nó Proxy
public class ProxyNode implements DFS {
	public static final String requestQueueName = "proxy_queue";
	public static final String messageSeparator = ",";
	//Id do nó proxy
	String id;
	
	//Mapa de réplicas
	ArrayList<ArrayList<String>> repmap = new ArrayList<ArrayList<String>>();
	
	//RPC
	Connection connection;
	Channel channel;
	Channel receiveChannel;
	Queue receiveQueue;
	String replyQueueName;
	String storageRequestQueueName = StorageNode.queueName;
	QueueingConsumer consumer;
	
	//Bindings
	//Receber mensagens do BalanceNode
	public static final String balanceBinding = "BALANCE";

	public ProxyNode(String id, String pathToMapFileString) throws IOException {
		this.id = id;
		parseMapFile(pathToMapFileString);
	}
	
	public void start() throws Exception {		
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    channel.exchangeDeclare(StorageNode.storageEx, "topic");

	    replyQueueName = channel.queueDeclare().getQueue();
	    channel.queueBind(replyQueueName, StorageNode.storageEx, "proxy_queue"+"."+id);
	    
	    consumer = new QueueingConsumer(channel);
	    channel.basicConsume(replyQueueName, true, consumer);
	    
		channel.queueDeclare(requestQueueName, false, false, false, null);
		channel.queueBind(requestQueueName, StorageNode.storageEx, "proxy_queue");
		channel.basicQos(1);

		
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(requestQueueName, false, consumer);

		System.out.println("Awaiting requests on [" + "proxy_queue"+"."+id + "]");

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
		    //Descobrir o ID
		    else if (messageArgs[0].equals("ID")){
		    	response += "That's my id";
		    }
		    //Mensagem inválida, retorna ERROR
		    else {
		    	response += "ERROR";
		    }
		    System.out.println("Sending response [" + response + "]");
		    
		    channel.basicPublish("", props.getReplyTo(), replyProps, response.getBytes());

		    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		}
		

	}
	
	private void parseMapFile(String pathToMapFileString) throws IOException {
		Path pathToMapFile = Paths.get(pathToMapFileString);
		String line = "";
		try (BufferedReader reader = Files.newBufferedReader(pathToMapFile)) {
			while ((line = reader.readLine()) != null){
				ArrayList<String> currArray = new ArrayList<String>(Arrays.asList(line.split(",")));
				this.repmap.add(currArray);
			}
			
			
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
			channel.basicPublish("", storageRequestQueueName + ".1", props, message.getBytes());
			System.out.println("Sent request for [" + storageRequestQueueName + ".1" + "] Awaiting response...");
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
			channel.basicPublish("", storageRequestQueueName + ".1", props, message.getBytes());
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
		if (args.length < 2) {
			System.out.println("Usage: ProxyNode [id] [path to map archive]");
		} else {
			ProxyNode proxyNode;
			proxyNode = new ProxyNode(args[0], args[1]);
			proxyNode.start();
//			proxyNode.create("lala.txt", "lalala");
//			System.out.println(proxyNode.read("lala.txt"));
		}
	}

}
