package leandro;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Principal {

	private Properties properties;
	private Boolean executarDDL = false;
	private Boolean executarInsert = false;
	private TipoBase tipoBase = TipoBase.ORACLE;

	public static void main(String[] args) throws SQLException {
		new Principal().execucao();
	}

	public void execucao() throws SQLException{
		Connection conn = this.getConnection();
		System.out.println("Conex�o aberta...");

		if (this.executarDDL){
			String sqlDDL = "create table pessoa (nome varchar(40));";
			this.ddl(conn, sqlDDL);
		}

		if (this.executarInsert){
			String sqlInsert = "insert into pessoa values (?)";
			if (this.insert(conn, sqlInsert)){
				System.out.println("Inseriu corretamente...");
			}
		}
		conn.close();
	}

	public boolean ddl(Connection conn, String sql){
		boolean result = false;
		try {
			Statement st = conn.createStatement();
			result = st.execute(sql);
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean insert(Connection conn, String sql){
		
		boolean retorno = false;
		
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, "Leandro");
			retorno = ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retorno;
	}

	public Connection getConnection(){
		try {
			Properties prop = getProperties();
			String urlMysql = prop.getProperty("urlMysql");
			String urlOracle = prop.getProperty("urlOracle");
			String user = prop.getProperty("username");
			String pass = prop.getProperty("password");
			
			Connection c = null;
			if (tipoBase.equals(TipoBase.ORACLE)){
				c = DriverManager.getConnection(urlOracle);
			}
			else if (tipoBase.equals(TipoBase.MYSQL)){
				c = DriverManager.getConnection(urlMysql, user, pass);
			}
			
			return c;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Properties getProperties(){

		if (properties == null){
			properties = new Properties();
			InputStream is;
			try {
				is = new FileInputStream("arq.properties");
				properties.load(is);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return this.properties;
	}
	
	enum TipoBase {
		MYSQL,
		ORACLE;
	}
}