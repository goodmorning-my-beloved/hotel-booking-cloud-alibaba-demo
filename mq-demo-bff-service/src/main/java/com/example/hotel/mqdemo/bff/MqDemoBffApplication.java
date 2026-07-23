package com.example.hotel.mqdemo.bff;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@EnableFeignClients
@SpringBootApplication
public class MqDemoBffApplication {

    public static void main(String[] args) {
        SpringApplication.run(MqDemoBffApplication.class, args);
    }
}
