package dfs;

import java.io.IOException;

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
	
	public void start() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String read(String name) {
		// TODO Auto-generated method stub
		return null;
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

}
