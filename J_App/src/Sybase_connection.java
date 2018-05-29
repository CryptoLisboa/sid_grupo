import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import sybase.jdbc4.sqlanywhere.*;

public class Sybase_connection {
	//private Sensor s;
	private String time;
	private Connection connect;
	private Statement stmn;
	private ResultSet rs;
	
	public void start() {
		int temp=5;
		int hum=4;
		try {
			System.out.println("vou ligar");
			connect=DriverManager.getConnection("jdbc:sqlanywhere:uid=DBA;pwd=sql;eng=SID_DB2-1;database=SID_DB2-1");
			System.out.println("j· liguei");
			stmn =connect.createStatement();
			rs=stmn.executeQuery("INSERT into HumidadeTemperatura(DataMed, HoraMed, ValorMedTemp, ValorMedHum, IDmedicao) VALUES (NULL, NULL,"+ temp +","+hum+",(select count(IDmedicao)+1 from HumidadeTemperatura))");
			System.out.println("criei");
			/*
			 * CONFIGURAR AQUI O INSERT STATEMENT PARA OS DADOS QUE VEM DO MONGO PARA UMA LISTA DO JAVA: Sybase_List
			 */
			/*for(int i=0;i<3;i++) {
			rs.next();
			System.out.println(rs.getString(3));
			}*/
		}catch(Exception e) {
			System.out.println("SYBASE OFF");
		}
	}
	public static void main(String[] args) {
		Sybase_connection db = new Sybase_connection();
		db.start();
	}
}