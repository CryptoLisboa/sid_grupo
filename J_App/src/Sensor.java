import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;


public class Sensor implements MqttCallback {
	
	private String topic;

	public Sensor(String topic) {
		this.topic = topic;
		corrida();
	}

	@Override
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void messageArrived(String topico, MqttMessage value) throws Exception {
		// TODO Auto-generated method stub
		String valueString = value.toString();
		JApp.getInstance().receiveSensorData(valueString);
	}
	
	private void corrida() {
		int qos = 0; // nao foi utilizado em nada
		String broker = "tcp://iot.eclipse.org:1883";
		String clientId = "sporting";

		try {
			MqttClient sampleClient = new MqttClient(broker, clientId);
			sampleClient.setCallback(this);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			sampleClient.connect(connOpts);
			sampleClient.subscribe(topic);
		} catch (MqttException me) {
			System.out.println("reason " + me.getReasonCode());
			System.out.println("msg " + me.getMessage());
			System.out.println("loc " + me.getLocalizedMessage());
			System.out.println("cause " + me.getCause());
			System.out.println("excep " + me);
			me.printStackTrace();
		}
	}

}
