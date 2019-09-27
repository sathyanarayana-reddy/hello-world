package com.spring.camel.kafka.demo;

import org.apache.camel.PropertyInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.springframework.stereotype.Component;

//@Component
public class CamelKafkaProps extends RouteBuilder {

	 @PropertyInject("kafka.server")
	 private String kafkaHost;

	 @PropertyInject("kafka.port")
	 private String kafkaPort;
	 
	 @PropertyInject("kafka.topic")
	 private String kafkaTopic;
	 
	 @PropertyInject("kafka.groupid")
	 private String kafkaGroupId;
	 
	 @PropertyInject("kafka.out.topic")
	 private String kafkaTopicOut;
	 
	@Override
	public void configure() throws Exception {

		KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
		kafkaConfiguration.setBrokers(kafkaHost + ":" + kafkaPort);
		kafkaConfiguration.setTopic(kafkaTopic);
		kafkaConfiguration.setGroupId(kafkaGroupId);
		
		KafkaEndpoint endpoint = new KafkaEndpoint("kafka", new KafkaComponent(getContext()));
		endpoint.setConfiguration(kafkaConfiguration);		
	 
		getContext().addEndpoint("kafka", endpoint);
		
		from("timer://senddata?period=1000")
				.routeId("kafkacamel.props.write")
				.setBody(constant("Message from Camel to Kafka .............."))
				.setHeader(KafkaConstants.KEY, constant("Camel"))
				.to("kafka");
		
		KafkaConfiguration kafkaConfiguration22 = new KafkaConfiguration();
		kafkaConfiguration22.setBrokers(kafkaHost + ":" + kafkaPort);
		kafkaConfiguration22.setTopic(kafkaTopicOut);
		kafkaConfiguration22.setGroupId(kafkaGroupId);
		
		KafkaEndpoint endpoint22 = new KafkaEndpoint("kafka22", new KafkaComponent(getContext()));
		
		endpoint22.setConfiguration(kafkaConfiguration22);
		getContext().addEndpoint("kafka22", endpoint22);
		
		from("timer://senddata2?period=1000")
				.routeId("kafkacamel.props.write.out")
				.setBody(constant("Message from Camel to Kafka .......##############......."))
				.setHeader(KafkaConstants.KEY, constant("Camel"))
				.to("kafka22");
		
		from("kafka")
				.routeId("kafkacamel.props.read")
				.log("Message received from Kafka : ${body}")
				.log("    on the topic ${headers[kafka.TOPIC]}")
				.log("    on the partition ${headers[kafka.PARTITION]}")
				.log("    with the offset ${headers[kafka.OFFSET]}")
				.log("    with the key ${headers[kafka.KEY]}");

		from("kafka22")
				.routeId("kafkacamel.props.read.out")
				.log("Message received from Kafka22 : ${body}")
				.log("    on the topic ${headers[kafka.TOPIC]}")
				.log("    on the partition ${headers[kafka.PARTITION]}")
				.log("    with the offset ${headers[kafka.OFFSET]}")
				.log("    with the key ${headers[kafka.KEY]}");
	}
}
