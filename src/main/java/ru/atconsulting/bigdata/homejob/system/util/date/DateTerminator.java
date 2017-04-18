package ru.atconsulting.bigdata.homejob.system.util.date;

import org.joda.time.DateTime;
import org.joda.time.Seconds;
import ru.atconsulting.bigdata.homejob.system.pojo.DimTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by NSkovpin on 15.04.2017.
 * Lambdas, i neeeeed you
 */
public class DateTerminator {

    private static final int ZERO = 0;
    private static final int SIX = 6;
    private static final int TEN = 10;
    private static final int EIGHTEEN = 18;
    private static final int TWENTY_THREE = 23;
    private static final int FIFTY_NINE = 59;
    private static final int NINE_HUNDRED_NINETY_NINE = 999;
    private static final List<Integer> WEEKENDS = Arrays.asList(6, 7);

    public static TimeSummary getTimeSummary(DateTime dateTimeStart, DateTime dateTimeEnd, Map<String, Integer> dimTime) {
        DateTime dateTime0000 = dateTimeStart.withTime(ZERO, ZERO, ZERO, ZERO);
        DateTime dateTime0600 = dateTimeStart.withTime(SIX, ZERO, ZERO, ZERO);
        DateTime dateTime1000 = dateTimeStart.withTime(TEN, ZERO, ZERO, ZERO);
        DateTime dateTime1800 = dateTimeStart.withTime(EIGHTEEN, ZERO, ZERO, ZERO);
        DateTime dateTime2300 = dateTimeStart.withTime(TWENTY_THREE, ZERO, ZERO, ZERO);
        DateTime dateTime2359 = dateTimeStart.withTime(TWENTY_THREE, FIFTY_NINE, FIFTY_NINE, NINE_HUNDRED_NINETY_NINE);

        TimeSummary timeSummary = new TimeSummary();
        timeSummary.incrementHome(getHomeSeconds(dateTimeStart, dateTimeEnd, dateTime0000, dateTime0600, dateTime2300, dateTime2359));
        timeSummary.incrementJob(getJobSeconds(dateTimeStart, dateTimeEnd, dateTime1000, dateTime1800, dimTime));
        timeSummary.incrementEvening(getEveningSeconds(dateTimeStart, dateTimeEnd, dateTime1800, dateTime2300, dimTime));
        timeSummary.incrementMorning(getMorningSeconds(dateTimeStart, dateTimeEnd, dateTime0600, dateTime1000, dimTime));
        timeSummary.incrementWeekend(getWeekendSeconds(dateTimeStart, dateTimeEnd, dateTime0600, dateTime2359, dateTime0000, dateTime2300, dimTime));
        timeSummary.incrementWeekendDay(getWeekendDaySeconds(dateTimeStart, dateTimeEnd, dateTime1000, dateTime1800, dimTime));
        timeSummary.incrementWeekendNight(getWeekendNightSeconds(dateTimeStart, dateTimeEnd, dateTime2300, dateTime2359, dateTime0000, dateTime0600, dimTime));
        return timeSummary;
    }

    public static TimeSummary getTimeSummary(List<DateIntervalMaker.GeoInterval> intervals, Map<String, Integer> dimTime) {
        TimeSummary timeSummary = new TimeSummary();
        for (DateIntervalMaker.GeoInterval geoInterval : intervals) {
            DateTime dateTime0000 = geoInterval.getStartInterval().withTime(ZERO, ZERO, ZERO, ZERO);
            DateTime dateTime0600 = geoInterval.getStartInterval().withTime(SIX, ZERO, ZERO, ZERO);
            DateTime dateTime1000 = geoInterval.getStartInterval().withTime(TEN, ZERO, ZERO, ZERO);
            DateTime dateTime1800 = geoInterval.getStartInterval().withTime(EIGHTEEN, ZERO, ZERO, ZERO);
            DateTime dateTime2300 = geoInterval.getStartInterval().withTime(TWENTY_THREE, ZERO, ZERO, ZERO);
            DateTime dateTime2359 = geoInterval.getStartInterval().withTime(TWENTY_THREE, FIFTY_NINE, FIFTY_NINE, NINE_HUNDRED_NINETY_NINE);

            timeSummary.incrementHome(getHomeSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime0000, dateTime0600, dateTime2300, dateTime2359));
            timeSummary.incrementJob(getJobSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime1000, dateTime1800, dimTime));
            timeSummary.incrementEvening(getEveningSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime1800, dateTime2300, dimTime));
            timeSummary.incrementMorning(getMorningSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime0600, dateTime1000, dimTime));
            timeSummary.incrementWeekend(getWeekendSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime0600, dateTime2359, dateTime0000, dateTime2300, dimTime));
            timeSummary.incrementWeekendDay(getWeekendDaySeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime1000, dateTime1800, dimTime));
            timeSummary.incrementWeekendNight(getWeekendNightSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime2300, dateTime2359, dateTime0000, dateTime0600, dimTime));
        }
        return timeSummary;
    }

    public static boolean isSameYearMonth(DateTime dateTimeFirst, DateTime dateTimeSecond) {
        return dateTimeFirst.getYear() == dateTimeSecond.getYear() && dateTimeFirst.getMonthOfYear() == dateTimeSecond.getMonthOfYear();
    }

    private static long getWeekendNightSeconds(DateTime dateTimeStart, DateTime dateTimeEnd,
                                               DateTime dt1, DateTime dt2, DateTime dt3, DateTime dt4, Map<String, Integer> dimTimeMap) {
        long weekendNight = 0;
        if (isHoliday(dateTimeStart, dimTimeMap) || dateTimeStart.getDayOfWeek() == 6) {
            weekendNight += getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
            weekendNight += getNightSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
            return weekendNight;
        } else if (dateTimeStart.getDayOfWeek() == 5) {
            return getNightSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
        } else if (dateTimeStart.getDayOfWeek() == 7) {
            return getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
        }
        return 0;
    }

    private static long getWeekendDaySeconds(DateTime dateTimeStart, DateTime dateTimeEnd,
                                             DateTime dt1, DateTime dt2, Map<String, Integer> dimTimeMap) {
        if (isHoliday(dateTimeStart, dimTimeMap) || isWeekend(dateTimeStart.getHourOfDay())) {
            return getDaySeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
        }
        return 0;
    }

    private static long getWeekendSeconds(DateTime dateTimeStart, DateTime dateTimeEnd,
                                          DateTime dt1, DateTime dt2, DateTime dt3, DateTime dt4, Map<String, Integer> dimTimeMap) {
        if (isHoliday(dateTimeStart, dimTimeMap) || dateTimeStart.getHourOfDay() == 7) {
            return getEarlyMorningBeforeNightSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
        } else if (dateTimeStart.getHourOfDay() == 6) {
            return getMorningBeforeEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
        }
        return 0;
    }

    private static long getJobSeconds(DateTime dateTimeStart, DateTime dateTimeEnd,
                                      DateTime startInterval, DateTime endInterval, Map<String, Integer> dimTime) {
        if (!isWeekend(dateTimeEnd.getDayOfWeek()) && !isHoliday(dateTimeStart, dimTime)) {
            return getDaySeconds(dateTimeStart, dateTimeEnd, startInterval, endInterval);
        }
        return 0;
    }

    private static long getHomeSeconds(DateTime dateTimeStart, DateTime dateTimeEnd,
                                       DateTime dt1, DateTime dt2, DateTime dt3, DateTime dt4) {
        long homeResult = 0;
        if (!isWeekend(dateTimeStart.getDayOfWeek())) {
            homeResult += getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
            homeResult += getNightSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
            return homeResult;
        } else {
            if (dateTimeStart.getDayOfWeek() == 6) {
                homeResult += getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
                return homeResult;
            } else {
                homeResult += getNightSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
                return homeResult;
            }
        }
    }

    private static long getNightSeconds(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isNight(dateTimeStart.getHourOfDay())) {
            if (isNight(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval.minusMillis(1)).getSeconds();
            }
        } else if (isNight(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getEveningSeconds(DateTime dateTimeStart, DateTime dateTimeEnd,
                                          DateTime startInterval, DateTime endInterval, Map<String, Integer> dimTime) {
        if (!isWeekend(dateTimeStart.getDayOfWeek()) && !isHoliday(dateTimeStart, dimTime)) {
            return getEveningSeconds(dateTimeStart, dateTimeEnd, startInterval, endInterval);
        }
        return 0;
    }

    private static long getEveningSeconds(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isEvening(dateTimeStart.getHourOfDay())) {
            if (isEvening(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval.minusMillis(1)).getSeconds();
            }
        } else if (isEvening(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getDaySeconds(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isDay(dateTimeStart.getHourOfDay())) {
            if (isDay(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval.minusMillis(1)).getSeconds();
            }
        } else if (isDay(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getEarlyMorningSeconds(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isEarlyMorning(dateTimeStart.getHourOfDay())) {
            if (isEarlyMorning(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval.minusMillis(1)).getSeconds();
            }
        } else if (isEarlyMorning(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getMorningSeconds(DateTime dateTimeStart, DateTime dateTimeEnd,
                                          DateTime startInterval, DateTime endInterval, Map<String, Integer> dimTime) {
        if (!isWeekend(dateTimeStart.getDayOfWeek()) && !isHoliday(dateTimeStart, dimTime)) {
            return getMorningSeconds(dateTimeStart, dateTimeEnd, startInterval, endInterval);
        }
        return 0;
    }

    private static long getMorningSeconds(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isMorning(dateTimeStart.getHourOfDay())) {
            if (isMorning(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval.minusMillis(1)).getSeconds();
            }
        } else if (isMorning(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getEarlyMorningBeforeNightSeconds(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isEarlyMorningBeforeNight(dateTimeStart.getHourOfDay())) {
            if (isEarlyMorningBeforeNight(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval.minusMillis(1)).getSeconds();
            }
        } else if (isEarlyMorningBeforeNight(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getMorningBeforeEarlyMorningSeconds(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isMorningBeforeEarlyMorning(dateTimeStart.getHourOfDay())) {
            if (isMorningBeforeEarlyMorning(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval.minusMillis(1)).getSeconds();
            }
        } else if (isMorningBeforeEarlyMorning(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getFullPeriodSeconds(DateTime dt1, DateTime dt2) {
        return Seconds.secondsBetween(dt1, dt2).getSeconds();
    }

    private static boolean isFullPeriod(DateTime dateTimeStart, DateTime dateTimeEnd, DateTime startInterval, DateTime endInterval) {
        return dateTimeStart.isBefore(startInterval) && dateTimeEnd.isAfter(endInterval);
    }

    private static boolean isEarlyMorning(int hour) {
        return ZERO <= hour && hour < SIX;
    }

    private static boolean isMorning(int hour) {
        return SIX <= hour && hour < TEN;
    }

    private static boolean isDay(int hour) {
        return TEN <= hour && hour < EIGHTEEN;
    }

    private static boolean isEvening(int hour) {
        return EIGHTEEN <= hour && hour < TWENTY_THREE;
    }

    private static boolean isNight(int hour) {
        return TWENTY_THREE == hour;
    }

    private static boolean isEarlyMorningBeforeNight(int hour) {
        return ZERO <= hour && hour < TWENTY_THREE;
    }

    private static boolean isMorningBeforeEarlyMorning(int hour) {
        return SIX <= hour && hour <= TWENTY_THREE;
    }

    private static boolean isWeekend(int day) {
        return WEEKENDS.contains(day);
    }

    private static boolean isHoliday(DateTime dateTime, Map<String, Integer> dimTime) {
        Integer index = dimTime.get(dateTime.toString(DimTime.Constants.DATE_FORMATTER_OUTPUT));
        return index != null && index == 1;
    }

}
