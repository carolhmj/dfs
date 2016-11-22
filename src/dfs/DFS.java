package dfs;

//Interface do sistema de arquivos, com as operações relativas à arquivos
public interface DFS {
	public static final String exchangeName = "dfs_exchange";
	boolean create(String name, String content);
	String read(String name);
}
