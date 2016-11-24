package dfs;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;

/*
 * Nó de balanceamento. Ele descobre o id de um proxy
 * e o retorna para o cliente
 */
public class BalanceNode {
	//Nome da fila onde ele vai receber requests dos clientes
	public static final String queueName = "balance_node";
	public static final String messageSeparator = ",";

	//Nome da fila de onde ele vai receber resposta dos proxys
	String responseQueueName;
	
	//RPC
	Connection connection;
	Channel channel;
	String proxyRequestQueueName = ProxyNode.queueBasicName;
	QueueingConsumer consumer;
	String ip = "localhost";
	public BalanceNode(String ip) {
		this.ip = ip;
	}
	
	public BalanceNode() {
	}

	public void start() throws Exception {		
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(ip);
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    channel.exchangeDeclare(DFS.exchangeName, "topic", false, false, false, null);

	    responseQueueName = channel.queueDeclare().getQueue(); 
	    consumer = new QueueingConsumer(channel);
	    channel.basicConsume(responseQueueName, true, consumer);
	    
		channel.queueDeclare(queueName, false, false, false, null);
		channel.queueBind(queueName, DFS.exchangeName, queueName);
		channel.basicQos(1);

		
		QueueingConsumer receptorConsumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, false, receptorConsumer);

		System.out.println("Awaiting requests on [" + queueName + "]");

		while (true) {
		    QueueingConsumer.Delivery delivery = receptorConsumer.nextDelivery();

		    BasicProperties props = delivery.getProperties();
		    AMQP.BasicProperties replyProps = new AMQP.BasicProperties().builder().correlationId(props.getCorrelationId()).build();
		    String message = new String(delivery.getBody());
		    String response = "";
		    
		    System.out.println("Received message [" + message + "]");
		    
		    //Parseia a mensagem, e cria a resposta
		    String[] messageArgs = message.split(messageSeparator);
		    //Cliente quer algum id de proxy
		    if (messageArgs[0].equals("ID")) {
		    	response += getNodeId();
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
	
	public String getNodeId() {
		String response = null;
	    String corrId = java.util.UUID.randomUUID().toString();

	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(responseQueueName)
	                                .build();

	    //Making the message
	    String message = "";
	    
	    message = message + "ID";
	    
	    
	    try {
	    	channel.basicPublish(DFS.exchangeName, "proxy_queue", props, message.getBytes());
			System.out.println("Sent request for [" + proxyRequestQueueName + "] Awaiting response...");
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
		BalanceNode balance;
		if (args.length == 0) {
			balance = new BalanceNode();
		} else {
			balance = new BalanceNode(args[0]);
		}
		balance.start();
	}
}
