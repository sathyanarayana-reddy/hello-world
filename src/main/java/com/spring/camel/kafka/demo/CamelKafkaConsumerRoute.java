package com.spring.camel.kafka.demo;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

//@Component
public class CamelKafkaConsumerRoute extends RouteBuilder{
	
  
	private static final Logger LOG = LoggerFactory.getLogger(CamelKafkaConsumerRoute.class);
	
	@Override
    public void configure() throws Exception {

		 LOG.info("About to run Kafka-camel consumer without SSL...");
		
		 from("kafka:brokers={{kafka.server}}:{{kafka.port}}?topic={{kafka.topic}}&groupId={{kafka.groupid}}")
        
		//from("kafka:brokers=localhost:9092?topic=cameltopic"
				
        //from("kafka:brokers=localhost:9093,localhost:9094,localhost:9095?topic=cameltopic"
        		//+ "&groupId=my-chanel")
                .routeId("kafkacamel.reader")
                .process(new Processor() {                  
                    public void process(Exchange exchange) throws Exception {
                        LOG.info("Message Body : " + exchange.getIn().getBody().toString());
                    }
                })
                .log("Message received from Kafka : ${body}")
                .log("    on the topic ${headers[kafka.TOPIC]}")
          	    .log("    on the partition ${headers[kafka.PARTITION]}")
          	    .log("    with the offset ${headers[kafka.OFFSET]}")
          	    .log("    with the key ${headers[kafka.KEY]}"); 
        
    }
	
    public static void main(String[] args) {       
    	CamelKafkaConsumerRoute routeBuilder = new CamelKafkaConsumerRoute();
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
