package dfs;

import java.io.IOException;

//Interface do sistema de arquivos, com as operações relativas à arquivos
public interface DFS {
	void create(String name, String content) throws IOException;
	String read(String name) throws IOException;
}
