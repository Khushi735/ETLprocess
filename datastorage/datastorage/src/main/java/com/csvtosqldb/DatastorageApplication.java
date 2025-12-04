package com.csvtosqldb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling // This turns on the "Alarm Clock" for your Scheduler
public class DatastorageApplication {

	public static void main(String[] args) {
		SpringApplication.run(DatastorageApplication.class, args);
	}

}