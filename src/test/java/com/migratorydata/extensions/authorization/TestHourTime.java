package com.migratorydata.extensions.authorization;

import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class TestHourTime {

    @Test
    public void test() {
        LocalDateTime start = LocalDateTime.now();
        // Hour + 1, set Minute and Second to 00
        LocalDateTime end = start.plusHours(1).truncatedTo(ChronoUnit.HOURS);

        // Get Duration
        Duration duration = Duration.between(start, end);
        long millis = duration.getSeconds();
        System.out.println(millis);
        int seconds = LocalDateTime.now().plusHours(1).toLocalTime().getMinute();
        System.out.println(seconds);
    }

}
