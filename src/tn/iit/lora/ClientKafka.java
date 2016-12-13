package tn.iit.lora;



public class ClientKafka extends DefaultKafka {

	public ClientKafka(String server, int port) {
		super("lora", "test2", 4, server, port);

	}

	@Override
	public void handle(String trame) {
		super.handle(trame);

	}

	public static void main(String[] args) {
		ClientKafka C = new ClientKafka("192.168.3.48", 9092);

		C.start();

	}

}
