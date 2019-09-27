package com.spring.camel.kafka.demo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.PropertyInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CamelKafkaProducerSSLRoute extends RouteBuilder{

	private static final Logger LOG = LoggerFactory.getLogger(CamelKafkaProducerSSLRoute.class);
	 @PropertyInject("kafka.server")
	 private String kafkaHost;

	 @PropertyInject("kafka.port")
	 private String kafkaPort;
	 
	 @PropertyInject("kafka.topic")
	 private String kafkaTopic;
	 
	 @PropertyInject("kafka.groupid")
	 private String kafkaGroupId;
	 
	 @PropertyInject("kafka.brokers.ssl")
	 private String kafkaBrokersSSL;
	 
	 @PropertyInject("kafka.brokers")
	 private String kafkaBrokers;
	 
	 @PropertyInject("kafka.out.topic")
	 private String kafkaTopicOut;
	 
	 @PropertyInject("kafka.securityProtocol")
	 private String securityProtocol;
	 
	 @PropertyInject("kafka.sslTruststoreLocation")
	 private String sslTruststoreLocation;
	 
	 @PropertyInject("kafka.sslTruststorePassword")
	 private String sslTruststorePassword;
	 
	 @PropertyInject("kafka.port.ssl")
	 private String kafkaPortSSL;
			 
	private ProducerTemplate producerTemplate;
    @Override
    public void configure() throws Exception {
    	
    	 LOG.info("About to run Kafka-camel producer with SSL...");
    	 
	        final StringBuilder kafkaUri = new StringBuilder("kafka:");
	        kafkaUri.append("brokers=" +kafkaHost).append(":").append(kafkaPortSSL).append("?topic=").append(kafkaTopic);
	        kafkaUri.append("&groupId=" + kafkaGroupId);	          
	          kafkaUri.append("&securityProtocol=" + securityProtocol);
	          kafkaUri.append("&sslTruststoreLocation=" + sslTruststoreLocation);
	          kafkaUri.append("&sslTruststorePassword=" + sslTruststorePassword); 
	        
/*		        final StringBuilder kafkaUri = new StringBuilder("kafka:");
		          kafkaUri.append(kafkaBrokersSSL).append("?topic=").append(kafkaTopic);	       
		          kafkaUri.append("&groupId=" + kafkaGroupId);	          
		          kafkaUri.append("&securityProtocol=" + securityProtocol);
		          kafkaUri.append("&sslTruststoreLocation=" + sslTruststoreLocation);
		          kafkaUri.append("&sslTruststorePassword=" + sslTruststorePassword);*/
		          
	         
	        		  
        from("timer://producer?period=1000")
        .routeId("kafkacamel.writer.ssl")
        .process(new Processor() {            
            public void process(Exchange exchange) throws Exception {
            	producerTemplate = exchange.getContext().createProducerTemplate();

                String message = UUID.randomUUID().toString();
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");  
                LocalDateTime now = LocalDateTime.now();
                message = message+" ------------------- sathya---------------"+dtf.format(now);
                
               // producerTemplate.sendBody("kafka:brokers=localhost:9096?topic=cameltopic"
                		
/*                producerTemplate.sendBody("kafka:brokers=localhost:9097,localhost:9098,localhost:9099?topic=cameltopic"
                        + "&groupId=my-chanel"
               		    + "&securityProtocol=SSL"
                        + "&sslTruststoreLocation=C:/Program Files/OpenSSL-Win64-1.1.1/bin/kafka.server.truststore.jks"
                        + "&sslTruststorePassword=test1234"
                		, message);*/
                
                producerTemplate.sendBody(kafkaUri.toString(),message);
                
            }
        });
    }
    public static void main(String[] args) {       
    	CamelKafkaProducerSSLRoute routeBuilder = new CamelKafkaProducerSSLRoute();
         CamelContext ctx = new DefaultCamelContext();
         try {
             ctx.addRoutes(routeBuilder);
             ctx.start();
             Thread.sleep(6 * 60 * 1000);
             ctx.stop();
         }
         catch (Exception e) {
             e.printStackTrace();
         }

     } 
}
