import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import sybase.jdbc4.sqlanywhere.*;

public class Sybase_connection {
	// private Sensor s;d
	private String time;
	private Connection connect;
	private Statement stmn;
	private ResultSet rs;

	public void start() {
		int temp = 5;
		int hum = 4;
		try {
			System.out.println("vou ligar");
			connect = DriverManager.getConnection("jdbc:sqlanywhere:uid=DBA;pwd=sql;eng=SID_DB2-1;database=SID_DB2-1");
			System.out.println("j· liguei");
			stmn = connect.createStatement();
			// obter id da proxima alinea para insercao
			ResultSet rs = stmn.executeQuery("select count(IDmedicao)+1 from HumidadeTemperatura");
			int queryIt = ((Number) rs.getObject(1)).intValue();
			// preparar o query da migracao de dados do mongodb para o sybase
			String queryMigration = createQuery(queryIt);
			rs = stmn.executeQuery(queryMigration);
			System.out.println("migrei os dados");
			// mover os dados da colecao temporaria para a permanente no mongodb
			JApp.getInstance().mongodbCollectionDataTransfer();
		} catch (Exception e) {
			System.out.println("SYBASE OFF");
		}
	}

	/*
	 * INSERT INTO MyTable ( Column1, Column2, Column3 ) VALUES ('John', 123,
	 * 'Lloyds Office'), ('Jane', 124, 'Lloyds Office'), ('Billy', 125, 'London
	 * Office'), ('Miranda', 126, 'Bristol Office');
	 */
	private String createQuery(int nextId) {
		int idLocal = nextId;
		// obter lista com dados a inserir no sybase
		List<String[]> migrationData = JApp.getInstance().getMigrationData();
		// criar uma variavel para cada coluna
		String localQuery = "INSERT into HumidadeTemperatura(DataMed, HoraMed, ValorMedTemp, ValorMedHum, IDmedicao) VALUES ";
		// iniciar a 1 porque o size começa a 1, caso nao esteja vazia
		int counter = 1;
		int list_size = migrationData.size();
		// percorrer a lista de dados afim de elaborar as variaveis com os dados das
		for (String[] data : migrationData) {

			String content = "(" + data[0] + ", " + data[1] + ", " + data[2] + ", " + data[3] + ", " + idLocal + ")";
			idLocal++;

			localQuery += content;
			// preparar para proxima entrada caso nao seja o ultimo
			if (counter < list_size) {
				localQuery += ", ";
			}
			counter++;
		}
		return localQuery;
	}
//
	public static void main(String[] args) {
		/*
		 * O elmo sugeriu mandar po caralho mais velho 
		 * Sybase_connection db = new
		 * Sybase_connection(); db.start();
		 */
	}
}