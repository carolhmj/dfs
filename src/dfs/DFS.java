package dfs;

//Interface do sistema de arquivos, com as operações relativas à arquivos
public interface DFS {
	boolean create(String name, String content);
	String read(String name);
}
