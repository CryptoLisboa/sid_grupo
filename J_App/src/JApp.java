import java.util.List;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class JApp {

	private static JApp instance = null;
	private MongoClient client;
	private DB db;
	private List<Sensor> sensores = new ArrayList<Sensor>();
	private List<String[]> mongo_list = new ArrayList<String[]>();
	protected long java_mongo_sleep = 15 * 1000;

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
		// master thread que garante que a slave thread está viva, senão tentar de N em
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
			DBCollection collection = db.getCollection("humidtemp_aux");

			/*
			 * Thread responsavel por transferir todos os dados que estejam em espera na lista do java para o mongo=>Collection=>humidtemp_aux
			 */
			new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						
						for (String[] vector_info : mongo_list) {
							String temperature = vector_info[0];
							String humidity = vector_info[1];
							String date = vector_info[2];
							String time = vector_info[3];
							
							DBObject sensor_data = new BasicDBObject("temperature", temperature).append("humidity", humidity)
									.append("date", date).append("time", time);
							DBCollection collection = db.getCollection("humidtemp_aux");
							collection.insert(sensor_data);
						}
						
						System.out.println("INSERI "+mongo_list.size()+" entradas no MONGO \n\n");
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

	}

	// https://eclipse.org/paho/clients/js/utility/
	// {“_id”:”36”, “temperature”:”36.0”, “humidity”: “72.3”, “date”: “02/03/2018”,
	// “time”: “01:00:00”}
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
				
				System.out.println("COLOQUEI DADOS NA LISTA DO JAVA em espera para o MONGO \n\n");
			}
		}).start();
	}
}