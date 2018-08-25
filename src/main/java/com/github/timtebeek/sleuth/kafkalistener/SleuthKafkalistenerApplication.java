package com.github.timtebeek.sleuth.kafkalistener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class SleuthKafkalistenerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SleuthKafkalistenerApplication.class, args);
    }

    @Scheduled(fixedRate = 10)
    public void logSomethingFromAnotherThread() {
        log.info("Periodic log message");
    }
}
