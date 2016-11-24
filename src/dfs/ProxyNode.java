package dfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
	public static final String queueBasicName = "proxy_queue";
	public static final String messageSeparator = ",";
	
	public String queueTrueName;
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
	String storageRequestQueueName = StorageNode.queueBasicName;
	QueueingConsumer consumer;

	public ProxyNode(String id, String pathToMapFileString) throws IOException {
		this.id = id;
		this.queueTrueName = queueBasicName + "." + id;
		parseMapFile(pathToMapFileString);

//		parseMapFile(pathToMapFileString);
	}
	
	public void start() throws Exception {		
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    channel.exchangeDeclare(DFS.exchangeName, "topic");

	    replyQueueName = channel.queueDeclare().getQueue();
	    channel.queueBind(replyQueueName, DFS.exchangeName, queueTrueName);
	    
	    consumer = new QueueingConsumer(channel);
	    channel.basicConsume(replyQueueName, true, consumer);
	    
		channel.queueDeclare(queueTrueName, false, false, false, null);
//		String queuePattern = "#.proxy_queue.#";
		channel.queueBind(queueTrueName, DFS.exchangeName, queueTrueName);
		channel.queueBind(queueTrueName, DFS.exchangeName, queueBasicName);
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
		    //Descobrir o ID
		    else if (messageArgs[0].equals("ID")){
		    	response += id;
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
	    ArrayList<String> ids = getIds(name);
	    try {
	    	for (String id : ids) {
				channel.basicPublish("", storageRequestQueueName + "." + id, props, message.getBytes());
				System.out.println("Sent request for [" + storageRequestQueueName + "." + ids + "] Awaiting response...");
	    	}
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
	    
	    ArrayList<String> ids = getIds(name);
	    try {
	    	for (String id : ids) {
				channel.basicPublish("", storageRequestQueueName + "." + id, props, message.getBytes());
				System.out.println("Sent request for [" + storageRequestQueueName + "." + ids + "] Awaiting response...");
	    	}
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
	/* Tira o hash MD5 do arquivo, divide pelo número de nós de armazenamento,
	 * procura os IDs no arquivo de réplicas e retorna os ids corretos
	 */
	public ArrayList<String> getIds(String arqName) {
		byte[] arqNameBytes;
		int lineIndex = 0;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			arqNameBytes = arqName.getBytes("UTF-8");
			BigInteger hashNumber = new BigInteger(md.digest(arqNameBytes));
			lineIndex = (hashNumber.remainder(BigInteger.valueOf(this.repmap.size()-1))).intValue();
			
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return this.repmap.get(lineIndex);

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
