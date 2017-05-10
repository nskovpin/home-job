package ru.atconsulting.bigdata.homejob.system;

import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Before;
import org.junit.Test;
import ru.atconsulting.bigdata.homejob.system.util.date.DateIntervalMaker;
import ru.atconsulting.bigdata.homejob.system.util.date.DateTerminator;
import ru.atconsulting.bigdata.homejob.system.util.date.TimeSummary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class TimeIntervalsTest {

    private Map<String, Integer> dimTimeTest;

    @Before
    public void setup(){
        dimTimeTest = new HashMap<>();
        dimTimeTest.put("20170308", 1);
        dimTimeTest.put("20170320", 1);
        dimTimeTest.put("20170321", 1);
    }

    @Test
    public void intervalsTest(){
        LocalDateTime dateTime1 = LocalDateTime.now();
        LocalDateTime dateTime2 = dateTime1.plusDays(2);

        List<DateIntervalMaker.GeoInterval> list =  DateIntervalMaker.tryToMakeDayIntervals(dateTime1, dateTime2);
        Assert.assertTrue(list.size() == 3);
        Assert.assertTrue(list.get(0).getStartInterval().equals(dateTime1));
    }

    @Test
    public void summaryTest(){
        LocalDateTime dateTime1 = LocalDateTime.parse("2017-05-02 10:52", DateTimeFormat.forPattern("yyyy-MM-dd hh:mm"));
        LocalDateTime dateTime2 = dateTime1.plusDays(2).minusHours(3).minusMinutes(10);

        List<DateIntervalMaker.GeoInterval> list =  DateIntervalMaker.tryToMakeDayIntervals(dateTime1, dateTime2);
        TimeSummary timeSummary = DateTerminator.getTimeSummary(list, dimTimeTest);
        Assert.assertTrue(timeSummary != null);
        Assert.assertTrue(timeSummary.getEvening()> 0);
    }

    @Test
    public void sameNightTest(){
        LocalDateTime dateTime1 = LocalDateTime.parse("2017-05-02 10:52", DateTimeFormat.forPattern("yyyy-MM-dd hh:mm"));
        LocalDateTime dateTime2 = dateTime1.plusDays(2).minusMinutes(10);

        List<DateIntervalMaker.GeoInterval> list =  DateIntervalMaker.tryToMakeDayIntervals(dateTime1, dateTime2);
        TimeSummary timeSummary = DateTerminator.getTimeSummary(list, dimTimeTest);
        Assert.assertTrue(timeSummary != null);
        Assert.assertTrue(timeSummary.getEvening()> 0);
        Assert.assertTrue(timeSummary.getHomeCount() == 2);
    }

    @Test
    public void sameWeekendTest(){
        LocalDateTime dateTime1 = LocalDateTime.parse("2017-05-05 10:52", DateTimeFormat.forPattern("yyyy-MM-dd hh:mm"));
        LocalDateTime dateTime2 = dateTime1.plusDays(3).minusMinutes(10);

        List<DateIntervalMaker.GeoInterval> list =  DateIntervalMaker.tryToMakeDayIntervals(dateTime1, dateTime2);
        TimeSummary timeSummary = DateTerminator.getTimeSummary(list, dimTimeTest);
        Assert.assertTrue(timeSummary != null);
        Assert.assertTrue(timeSummary.getEvening()> 0);
        Assert.assertTrue(timeSummary.getHomeCount() == 2);
        Assert.assertTrue(timeSummary.getWeekendNightCount() == 2);

    }

}
