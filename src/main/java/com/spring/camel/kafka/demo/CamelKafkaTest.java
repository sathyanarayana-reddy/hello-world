package com.spring.camel.kafka.demo;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.springframework.stereotype.Component;

//@Component
public class CamelKafkaTest extends RouteBuilder {

/*	 @PropertyInject("kafka.server")
	 private String kafkaHost;

	 @PropertyInject("kafka.port")
	 private String kafkaPort;
	 
	 @PropertyInject("kafka.topic")
	 private String kafkaTopic;
	 
	 @PropertyInject("kafka.groupid")
	 private String kafkaGroupId;*/
	 
	@Override
	public void configure() throws Exception {

	    KafkaEndpoint endpoint = createKafkaEndpoint();	    
		getContext().addEndpoint("kafka", endpoint);
		
		from("timer://senddata?period=1000")
				.setBody(constant("Message from Camel to Kafka .....========================----------..."))
				.setHeader(KafkaConstants.KEY, constant("Camel"))
				.to("kafka");
		
		from("kafka")
				.log("Message received from Kafka : ${body}")
				.log("    on the topic ${headers[kafka.TOPIC]}")
				.log("    on the partition ${headers[kafka.PARTITION]}")
				.log("    with the offset ${headers[kafka.OFFSET]}")
				.log("    with the key ${headers[kafka.KEY]}");

	}

	public static void main(String[] args) {
		CamelKafkaTest routeBuilder = new CamelKafkaTest();
		CamelContext ctx = new DefaultCamelContext();
		try {
			
			//KafkaComponent kafka = routeBuilder.createKafkaComponent();		    
		    //ctx.addComponent("kafka", kafka);
		    
		   // KafkaEndpoint endpoint = routeBuilder.createKafkaEndpoint();
		   // ctx.addEndpoint("kafka", endpoint);	   		

			
			ctx.addRoutes(routeBuilder);
			ctx.start();
			Thread.sleep(6 * 60 * 1000);
			ctx.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	
	public  KafkaEndpoint createKafkaEndpoint() {

		String kafkaHost = "localhost";
		String kafkaPort = "9092";
		String kafkaTopic = "cameltopic";
		String kafkaGroupId = "my-chanel";
		
		KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();

		kafkaConfiguration.setBrokers(kafkaHost + ":" + kafkaPort);
		kafkaConfiguration.setTopic(kafkaTopic);
		kafkaConfiguration.setGroupId(kafkaGroupId);
		
		KafkaEndpoint endpoint = new KafkaEndpoint("kafka", new KafkaComponent(getContext()));
		endpoint.setConfiguration(kafkaConfiguration);

		return endpoint;
	}

	public  KafkaComponent createKafkaComponent() {
		
		String kafkaHost = "localhost";
		String kafkaPort = "9092";
		String kafkaTopic = "cameltopic";
		String kafkaGroupId = "my-chanel";
		
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
        kafkaConfiguration.setBrokers(kafkaHost + ":" + kafkaPort);
        kafkaConfiguration.setTopic(kafkaTopic);
        kafkaConfiguration.setGroupId(kafkaGroupId);
        
		//KafkaComponent kafkaComponent = new KafkaComponent();		
		//kafkaComponent.setConfiguration(kafkaConfiguration);	
		
		KafkaComponent kafkaComponent = new KafkaComponent();
        //kafkaComponent.setEndpointClass(createKafkaEndpoint().getClass());
                
		return kafkaComponent;
	}

}
