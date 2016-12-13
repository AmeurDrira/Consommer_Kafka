package tn.iit.lora;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.utils.ShutdownableThread;

public class DefaultKafka extends ShutdownableThread {
	String server;
	int port;

	KafkaConsumer<Integer, String> consumer;
	String topic;
	int groupid;
	String clientid;

	public DefaultKafka(String topic, String clientid, int groupid, String server, int port) {
		super("KafkaConsumerExample", false);
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server + ":" + port);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer" + groupid);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientid);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("kafka.topic.wildcard.match", true);
		// props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

		consumer = new KafkaConsumer<>(props);
		this.topic = topic;
		this.groupid = groupid;
		this.clientid = clientid;
		System.out.println("topics:" + consumer.listTopics().keySet());

	}

	public void handle(String trame) {
		System.out.println("kafka.DefaultConsumer.handle : " + trame);
	}

	public void doWork() {
		consumer.subscribe(Collections.singletonList(this.topic));

		ConsumerRecords<Integer, String> records = consumer.poll(100);
		for (ConsumerRecord<Integer, String> record : records) {

			handle(record.value().toString());

		}
	}

}