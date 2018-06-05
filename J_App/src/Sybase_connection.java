import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import sybase.jdbc4.sqlanywhere.*;

public class Sybase_connection {
	// private Sensor s;d
	private String time;
	private Connection connect;
	private Statement stmn;
	private ResultSet rs;

	public void start() {
		try {
			System.out.println("vou ligar");
			connect = DriverManager.getConnection("jdbc:sqlanywhere:uid=Sensor;pwd=java;eng=SID_2;database=SID_2");
			System.out.println("já liguei");
			stmn = connect.createStatement();
			// obter id da proxima alinea para insercao
			// "select count(IDmedicao)+1 as total from HumidadeTemperatura"
			ResultSet rs = stmn.executeQuery("CALL DBA.getNextHumTempID()");
			int italico = 0;
			while (rs.next()) {
				italico = rs.getInt("IDMedicao");
			}
			connect.close();
			System.out.println("Italico == " + italico);

			int queryIt = italico;
			// preparar o query da migracao de dados do mongodb para o sybase
			String queryMigration = createQuery(queryIt);
			if (queryMigration.length() > 140) {
				connect = DriverManager.getConnection("jdbc:sqlanywhere:uid=Sensor;pwd=java;eng=SID_2;database=SID_2");
				stmn = connect.createStatement();
				System.out.println("a executar o segundo query\n\n" + queryMigration + "\n\n");
				rs = stmn.executeQuery(queryMigration);
				System.out.println("migrei os dados");
				// mover os dados da colecao temporaria para a permanente no mongodb
				JApp.getInstance().mongodbCollectionDataTransfer();
			}
			connect.close();
		} catch (Exception e) {
			System.out.println("SYBASE OFF");
			System.out.println(e.getMessage());
		}
	}

	public static void main(String[] args) {
		Sybase_connection sb = new Sybase_connection();
		sb.start();
	}

	public static String formatDate(String date, String initDateFormat, String endDateFormat) {

		Date initDate = null;
		try {
			initDate = new SimpleDateFormat(initDateFormat).parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		SimpleDateFormat formatter = new SimpleDateFormat(endDateFormat);
		String parsedDate = formatter.format(initDate);

		return parsedDate;
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
		String localQuery = "INSERT into DBA.HumidadeTemperatura(ValorMedicaoTemperatura, ValorMedicaoHumidade, DataMedicao, HoraMedicao, IDMedicao) VALUES ";
		// iniciar a 1 porque o size começa a 1, caso nao esteja vazia
		// iniciar a 1 porque o size comeÃ§a a 1, caso nao esteja vazia
		int counter = 1;
		int list_size = migrationData.size();

		// percorrer a lista de dados afim de elaborar as variaveis com os dados das
		/*
		 * 
		 * vector_info[0] = temperature; vector_info[1] = humidity; vector_info[2] =
		 * date; vector_info[3] = time;
		 */
		for (String[] data : migrationData) {
			String date = data[2];
			String dateFOrmatada = formatDate(date, "dd/mm/yyyy", "yyyy-mm-dd");
			String content = "(" + data[0] + ", " + data[1] + ", " + "'" + dateFOrmatada + "'" + ", " + "'" + data[3]
					+ "'" + ", " + idLocal + ")";
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
	//

}