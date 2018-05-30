import java.util.List;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class JApp {

	private static JApp instance = null;
	private MongoClient client;
	private DB db;
	private List<Sensor> sensores = new ArrayList<Sensor>();
	private List<String[]> mongo_list = new ArrayList<String[]>(), migration_list;
	protected long java_mongo_sleep = 15 * 1000, mongo_sybase_sleep = 30 * 1000;

	protected JApp() {
	}

	public static JApp getInstance() {
		if (instance == null)
			instance = new JApp();
		return instance;
	}

	void run() {
		startPahoLink();
		startMongoLink();
		startSybaseLink();
	}

	@SuppressWarnings({ "deprecation" })
	private void startMongoLink() {
		// master thread que garante que a slave thread estÃ¡ viva, senÃ£o tentar de N em
		// N segundos/minutos

		// tarefas da slave thread
		{
			// connect to Mongo
			client = new MongoClient("localhost", 27017);
			String connectPoint = client.getConnectPoint();
			System.out.println(connectPoint);
			// connect to database
			db = client.getDB("sid");
			// get collection
			//DBCollection collection = db.getCollection("humidtemp_aux");

			/*
			 * Thread responsavel por transferir todos os dados que estejam em espera na
			 * lista do java para o mongo=>Collection=>humidtemp_aux
			 */
			new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						DBCollection collection = db.getCollection("humidtemp_aux");

						for (String[] vector_info : mongo_list) {
							String temperature = vector_info[0];
							String humidity = vector_info[1];
							String date = vector_info[2];
							String time = vector_info[3];

							DBObject sensor_data = new BasicDBObject("temperature", temperature)
									.append("humidity", humidity).append("date", date).append("time", time);
							collection.insert(sensor_data);
						}

						System.out.println("INSERI " + mongo_list.size() + " entradas no MONGO \n\n");

						mongo_list.clear();
						// esperar por mais dados
						try {
							Thread.currentThread().sleep(java_mongo_sleep);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}).start();
		}
	}

	private void startPahoLink() {
		String topic1;
		// topic1 = "sid_lab_2018";
		topic1 = "sportingcampeao";
		sensores.add(new Sensor(topic1));
	}

	private void startSybaseLink() {
		// fazer thread
		Sybase_connection sybase_connection = new Sybase_connection();
		new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					sybase_connection.start();
					try {
						Thread.currentThread().sleep(mongo_sybase_sleep);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}				
			}
		}).start();
	}

	// https://eclipse.org/paho/clients/js/utility/
	// {â€œ_idâ€�:â€�36â€�, â€œtemperatureâ€�:â€�36.0â€�, â€œhumidityâ€�: â€œ72.3â€�, â€œdateâ€�: â€œ02/03/2018â€�,
	// â€œtimeâ€�: â€œ01:00:00â€�}
	public void receiveSensorData(String value) {
		// iniciar thread que introduz dados no mongo
		new Thread(new Runnable() {
			@Override
			public void run() {
				String[] array = value.split("\"");

				String temperature = array[3];
				String humidity = array[7];
				String date = array[11];
				String time = array[15];

				String[] vector_info = new String[4];

				vector_info[0] = temperature;
				vector_info[1] = humidity;
				vector_info[2] = date;
				vector_info[3] = time;

				mongo_list.add(vector_info);

				//System.out.println("COLOQUEI DADOS NA LISTA DO JAVA em espera para o MONGO \n\n");
			}
		}).start();
	}

	public List<String[]> getMigrationData() {
		DBCollection collection = db.getCollection("humidtemp_aux");
		DBCursor cursor = collection.find();
		migration_list = new ArrayList<String[]>();
		
		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			String temperature = (String) obj.get("temperature");
			String humidity = (String) obj.get("humidity");
			String date = (String) obj.get("date");
			String time = (String) obj.get("time");
			
			String[] vector_info = new String[4];

			vector_info[0] = temperature;
			vector_info[1] = humidity;
			vector_info[2] = date;
			vector_info[3] = time;

			migration_list.add(vector_info);			
		}
		return migration_list;
	}

	public void mongodbCollectionDataTransfer() {
		// obter dados a tranferir
		DBCollection auxColl = db.getCollection("humidtemp_aux");
		// criar lista em formato compativel com os dados
		List<DBObject> object_list = new ArrayList<DBObject>();
		// obter cursor associado a colecao
		DBCursor cursor = auxColl.find();
		// percorrer colecao copiando os dados
		while (cursor.hasNext()) {
			DBObject dbObject = cursor.next();
			object_list.add(dbObject);
		}
		// obter colecao permanente dos dados na mongodb
		DBCollection perm_coll = db.getCollection("humidtemp");
		// inserir dados da lista na colecao permanente
		perm_coll.insert(object_list);
		// eliminar colecao temporaria dos dados
		auxColl.drop();
		// recriar colecao temporaria para uso futuro
		db.getCollection("humidtemp_aux");
	}
}