package com.spring.camel.kafka.demo;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

//@Component
public class FileComponentRoute extends RouteBuilder {

	@Override
	public void configure() throws Exception {
		from("file:C://inputFolder?noop=true").to("file:C://outputFolder");
	}
}
