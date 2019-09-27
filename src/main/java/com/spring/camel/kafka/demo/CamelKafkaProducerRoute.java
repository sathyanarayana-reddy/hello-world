package com.spring.camel.kafka.demo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;  

//@Component
public class CamelKafkaProducerRoute extends RouteBuilder{

	private static final Logger LOG = LoggerFactory.getLogger(CamelKafkaProducerRoute.class);
	
	private ProducerTemplate producerTemplate;
    @Override
    public void configure() throws Exception {
    	
    	 LOG.info("About to run Kafka-camel producer without SSL.! ");
    	 
        from("timer://producer?period=1000")
        .routeId("kafkacamel.writer")
        .process(new Processor() {            
            public void process(Exchange exchange) throws Exception {
            	producerTemplate = exchange.getContext().createProducerTemplate();

                String message = UUID.randomUUID().toString();
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");  
                LocalDateTime now = LocalDateTime.now();  
                message = message+" -- "+dtf.format(now);
                
               // producerTemplate.sendBody("kafka:brokers=localhost:9093,localhost:9094,localhost:9095?topic=cameltopic", message);
                
                producerTemplate.sendBody("kafka:brokers=localhost:9092?topic=cameltopic", message);
                
            }
        });
    }
    public static void main(String[] args) {       
    	CamelKafkaProducerRoute routeBuilder = new CamelKafkaProducerRoute();
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
