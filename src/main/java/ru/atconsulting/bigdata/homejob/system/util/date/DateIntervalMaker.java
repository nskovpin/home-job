package ru.atconsulting.bigdata.homejob.system.util.date;

import lombok.Getter;
import lombok.Setter;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class DateIntervalMaker {

    public static List<GeoInterval> tryToMakeDayIntervals(DateTime startInput, DateTime endInput) {
        List<GeoInterval> geoIntervals = new ArrayList<>();
        DateTime start = startInput.withTimeAtStartOfDay();
        for (; !start.isAfter(endInput); start = start.plusDays(1)) {
            GeoInterval geoInterval = new GeoInterval();
            if (start.getDayOfMonth() == startInput.getDayOfMonth()) {
                geoInterval.setStartInterval(startInput);
            } else {
                geoInterval.setStartInterval(start.withTimeAtStartOfDay());
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

    private static boolean isSameTime(DateTime dateTime1, DateTime dateTime2){
        return dateTime1.getMillis() == dateTime2.getMillis();
    }

    @Setter
    @Getter
    public static class GeoInterval {

        private DateTime startInterval;
        private DateTime endInterval;
    }

}
