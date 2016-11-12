package dfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Tuple;

import static java.nio.file.StandardOpenOption.*;

//Classe que representa um nó de armazenamento
public class StorageNode implements DFS{
	// Id do nó de armazenamento, é o mesmo do mapa de réplicas
	String id;
	//Os arquivos serão criados em uma pasta em path
	Path path;
	
	//Parte remota
	JChannel channel;
	RpcDispatcher dispatcher;
	
	
	public StorageNode(String id) throws IOException {
		this.id = id;
		String pathString = id;
		this.path = Paths.get(pathString);
		//Cria uma pasta com o id do nó de armazenamento
		Files.createDirectories(path);
	}
	
	public void start() throws Exception {
		channel = new JChannel();
		channel.connect("StorageNodeCluster");
		dispatcher = new RpcDispatcher(channel, this);
		
		while(true) {
			
		}
		
//		dispatcher.close();
//		channel.close();
	}
	
	public StorageNode(String id, String pathToDir) throws IOException {
		this.id = id;
		Path toDir = Paths.get(pathToDir);
		this.path = toDir.resolve(id);
		//Cria uma pasta com o id do nó de armazenamento
		Files.createDirectories(path);
	}
	
	public StorageNode() { }

	public Tuple<String, Address> getIdAddress() {
		return new Tuple<String, Address>(id, channel.getAddress());
	}
	
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
			
//			while (true) {}
			
//			//Parte remota

//			
//			
//			try {
//				MethodCall createCall = new MethodCall(node.getClass().getMethod("create", String.class, String.class));
//				MethodCall readCall = new MethodCall(node.getClass().getMethod("read", String.class));
//			} catch (NoSuchMethodException | SecurityException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			

		}
	}
}
