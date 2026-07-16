package com.example.hotel.sentinel.a;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@EnableFeignClients
@SpringBootApplication
public class SentinelAApplication {

    public static void main(String[] args) {
        SpringApplication.run(SentinelAApplication.class, args);
    }
}
