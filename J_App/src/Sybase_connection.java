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
			connect = DriverManager.getConnection("jdbc:sqlanywhere:uid=dba;pwd=sql;eng=SID_DB2-1;database=SID_DB2-1");
			System.out.println("j· liguei");
			stmn = connect.createStatement();
			// obter id da proxima alinea para insercao
			ResultSet rs = stmn.executeQuery("select count(IDmedicao)+1 as total from HumidadeTemperatura");
			int italico = 0;
			while (rs.next()) {
				italico = rs.getInt("total");
			}
			System.out.println("Italico == " + italico);
			//
			int queryIt = italico+1;
			System.out.println("picha : " + queryIt);
			// preparar o query da migracao de dados do mongodb para o sybase
			String queryMigration = createQuery(queryIt);
			System.out.println("a executar o segundo query\n\n" + queryMigration + "\n\n");
			rs = stmn.executeQuery(queryMigration);
			System.out.println("migrei os dados");
			// mover os dados da colecao temporaria para a permanente no mongodb
			JApp.getInstance().mongodbCollectionDataTransfer();
		} catch (Exception e) {
			System.out.println("SYBASE OFF");
		}
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
		String localQuery = "INSERT into HumidadeTemperatura(ValorMedTemp, ValorMedHum, DataMed, HoraMed, IDmedicao) VALUES ";
		// iniciar a 1 porque o size começa a 1, caso nao esteja vazia
		int counter = 1;
		int list_size = migrationData.size();
		// percorrer a lista de dados afim de elaborar as variaveis com os dados das
		/*
		 * 
		 * vector_info[0] = temperature; 
		 * vector_info[1] = humidity; 
		 * vector_info[2] = date;
		 * vector_info[3] = time;
		 */
		for (String[] data : migrationData) {
			String date = data[2];
			String dateFOrmatada = formatDate(date, "dd/mm/yyyy", "yyyy-MM-dd");
			String content = "(" + data[0] + ", " + data[1] + ", " + dateFOrmatada + ", " + "'" + data[3] + "'" + ", " + idLocal
					+ ")";
			idLocal++;

			localQuery += content;
			break;
			// preparar para proxima entrada caso nao seja o ultimo
			/*
			if (counter < list_size) {
				localQuery += ", ";
			}
			counter++;
			*/
		}
		return localQuery;
	}
	//
	//

}