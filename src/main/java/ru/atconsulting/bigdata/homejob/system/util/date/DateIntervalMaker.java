package ru.atconsulting.bigdata.homejob.system.util.date;

import lombok.Getter;
import lombok.Setter;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class DateIntervalMaker {

    public static List<GeoInterval> tryToMakeDayIntervals(LocalDateTime startInput, LocalDateTime endInput) {
        List<GeoInterval> geoIntervals = new ArrayList<>();
        LocalDateTime start = startInput.withTime(0,0,0,0);
        for (; !start.isAfter(endInput); start = start.plusDays(1)) {
            GeoInterval geoInterval = new GeoInterval();
            if (start.getDayOfMonth() == startInput.getDayOfMonth()) {
                geoInterval.setStartInterval(startInput);
            } else {
                geoInterval.setStartInterval(start.withTime(0,0,0,0));
            }
            if (endInput.getDayOfMonth() == start.getDayOfMonth()) {
                geoInterval.setEndInterval(endInput);
            } else {
                geoInterval.setEndInterval(start.withTime(23, 59, 59, 999));
            }
            if(!isSameTime(geoInterval.getStartInterval(), geoInterval.endInterval)){
                geoIntervals.add(geoInterval);
            }
        }
        return geoIntervals;
    }

    private static boolean isSameTime(LocalDateTime dateTime1, LocalDateTime dateTime2){
        return dateTime1.equals(dateTime2);
    }

    @Setter
    @Getter
    public static class GeoInterval {

        private LocalDateTime startInterval;
        private LocalDateTime endInterval;
    }

}
