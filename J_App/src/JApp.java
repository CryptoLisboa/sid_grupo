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

	protected JApp() {
	}

	public static JApp getInstance() {
		if (instance == null)
			instance = new JApp();
		return instance;
	}

	void run() {
		connectMongo();
		startPahoLink();
		startSybaseLink();
	}

	@SuppressWarnings({ "deprecation" })
	private void connectMongo() {
		// master thread que garante que a slave thread está viva, senão tentar de N em N segundos/minutos
		
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

				DBObject person = new BasicDBObject("temperature", temperature).append("humidity", humidity)
						.append("date", date).append("time", time);
				DBCollection collection = db.getCollection("humidtemp_aux");
				collection.insert(person);

				System.out.println("Tratamos do " + "\n temperature " + temperature + "\n humidity " + humidity
						+ "\n date " + date + "\n time " + time);
			}
		}).start();
	}
}