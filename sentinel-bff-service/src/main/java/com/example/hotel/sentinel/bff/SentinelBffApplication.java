package com.example.hotel.sentinel.bff;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@EnableFeignClients
@SpringBootApplication
public class SentinelBffApplication {

    public static void main(String[] args) {
        SpringApplication.run(SentinelBffApplication.class, args);
    }
}
