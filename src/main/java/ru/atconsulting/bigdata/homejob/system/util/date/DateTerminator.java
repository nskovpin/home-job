package ru.atconsulting.bigdata.homejob.system.util.date;

import org.joda.time.LocalDateTime;
import org.joda.time.Seconds;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair.Pair;
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


    public static TimeSummary getTimeSummary(List<DateIntervalMaker.GeoInterval> intervals, Map<String, Integer> dimTime) {
        TimeSummary timeSummary = new TimeSummary();
        LocalDateTime previousEnd = null;
        DayPeriodEnum previousHome = DayPeriodEnum.NONE;
        DayPeriodEnum previousWeekendNight = DayPeriodEnum.NONE;

        for (DateIntervalMaker.GeoInterval geoInterval : intervals) {
            LocalDateTime dateTime0000 = geoInterval.getStartInterval().withTime(ZERO, ZERO, ZERO, ZERO);
            LocalDateTime dateTime0600 = geoInterval.getStartInterval().withTime(SIX, ZERO, ZERO, ZERO);
            LocalDateTime dateTime1000 = geoInterval.getStartInterval().withTime(TEN, ZERO, ZERO, ZERO);
            LocalDateTime dateTime1800 = geoInterval.getStartInterval().withTime(EIGHTEEN, ZERO, ZERO, ZERO);
            LocalDateTime dateTime2300 = geoInterval.getStartInterval().withTime(TWENTY_THREE, ZERO, ZERO, ZERO);
            LocalDateTime dateTime2359 = geoInterval.getStartInterval().withTime(TWENTY_THREE, FIFTY_NINE, FIFTY_NINE, NINE_HUNDRED_NINETY_NINE);

            Pair<Long, DayPeriodEnum> homeResult = getHomeSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime0000, dateTime0600, dateTime2300, dateTime2359, dimTime);
            if (isSameNight(previousHome, previousEnd, dateTime2359)) {
                incrementHomeWithNight(homeResult, timeSummary);
            } else {
                timeSummary.incrementHome(homeResult.getKey());
            }

            timeSummary.incrementJob(getJobSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime1000, dateTime1800, dimTime));
            timeSummary.incrementEvening(getEveningSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime1800, dateTime2300, dimTime));
            timeSummary.incrementMorning(getMorningSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime0600, dateTime1000, dimTime));
            timeSummary.incrementWeekend(getWeekendSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime0600, dateTime2359, dateTime0000, dateTime2300, dimTime));
            timeSummary.incrementWeekendDay(getWeekendDaySeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime1000, dateTime1800, dimTime));

            Pair<Long, DayPeriodEnum> weekendNightResult = getWeekendNightSeconds(geoInterval.getStartInterval(), geoInterval.getEndInterval(), dateTime2300, dateTime2359, dateTime0000, dateTime0600, dimTime);
            if (isSameNight( previousWeekendNight, previousEnd, dateTime2359)) {
                incrementWeekendNightWithNight(weekendNightResult, timeSummary);
            } else {
                timeSummary.incrementWeekendNight(weekendNightResult.getKey());
            }

            previousEnd = geoInterval.getEndInterval();
            previousHome = homeResult.getValue();
            previousWeekendNight = weekendNightResult.getValue();
        }
        return timeSummary;
    }


    public static boolean isSameYearMonth(LocalDateTime dateTimeFirst, LocalDateTime dateTimeSecond) {
        return dateTimeFirst.getYear() == dateTimeSecond.getYear() && dateTimeFirst.getMonthOfYear() == dateTimeSecond.getMonthOfYear();
    }

    private static void incrementHomeWithNight(Pair<Long, DayPeriodEnum> result, TimeSummary timeSummary){
        switch (result.getValue()){
            case EARLY_MORNING :{
                timeSummary.incrementHome(result.getKey() + 1, true);
                break;
            }
            case BOTH:{
                timeSummary.incrementHome(result.getKey() + 1);
                break;
            }
            case NIGHT:{
                timeSummary.incrementHome(result.getKey());
                break;
            }
        }
    }

    private static void incrementWeekendNightWithNight(Pair<Long, DayPeriodEnum> result, TimeSummary timeSummary){
        switch (result.getValue()){
            case EARLY_MORNING :{
                timeSummary.incrementWeekendNight(result.getKey() + 1, true);
                break;
            }
            case BOTH:{
                timeSummary.incrementWeekendNight(result.getKey() + 1);
                break;
            }
            case NIGHT:{
                timeSummary.incrementWeekendNight(result.getKey());
                break;
            }
        }
    }

    private static boolean isSameNight(DayPeriodEnum previousDayPeriod, LocalDateTime previousEnd, LocalDateTime dateTime2359) {
        if (previousEnd != null && previousEnd.equals(dateTime2359.minusDays(1))) {
            if (previousDayPeriod.equals(DayPeriodEnum.NIGHT) || previousDayPeriod.equals(DayPeriodEnum.BOTH)) {
                return true;
            }
        }
        return false;
    }

    private static Pair<Long, DayPeriodEnum> getWeekendNightSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd,
                                                                    LocalDateTime dt1, LocalDateTime dt2, LocalDateTime dt3, LocalDateTime dt4, Map<String, Integer> dimTimeMap) {
        long weekendNight = 0;
        DayPeriodEnum dayPeriodEnum = DayPeriodEnum.NONE;
        if (isHoliday(dateTimeStart, dimTimeMap) || dateTimeStart.getDayOfWeek() == 6) {
            long earlyMorning = getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
            long night = getNightSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
            weekendNight += earlyMorning;
            weekendNight += night;
            return Pair.of(weekendNight, resolveDayPeriodEnum(earlyMorning, night));
        } else if (dateTimeStart.getDayOfWeek() == 5) {
            weekendNight += getNightSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
            if (weekendNight > 0) {
                dayPeriodEnum = DayPeriodEnum.NIGHT;
            }
            return Pair.of(weekendNight, dayPeriodEnum);
        } else if (dateTimeStart.getDayOfWeek() == 7) {
            weekendNight += getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
            if (weekendNight > 0) {
                dayPeriodEnum = DayPeriodEnum.EARLY_MORNING;
            }
            return Pair.of(weekendNight, dayPeriodEnum);
        }
        return Pair.of(weekendNight, dayPeriodEnum);
    }

    private static long getWeekendDaySeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd,
                                             LocalDateTime dt1, LocalDateTime dt2, Map<String, Integer> dimTimeMap) {
        if (isHoliday(dateTimeStart, dimTimeMap) || isWeekend(dateTimeStart.getDayOfWeek())) {
            return getDaySeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
        }
        return 0;
    }

    private static long getWeekendSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd,
                                          LocalDateTime dt1, LocalDateTime dt2, LocalDateTime dt3, LocalDateTime dt4, Map<String, Integer> dimTimeMap) {
        if (isHoliday(dateTimeStart, dimTimeMap) || dateTimeStart.getDayOfWeek() == 7) {
            return getEarlyMorningBeforeNightSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
        } else if (dateTimeStart.getDayOfWeek() == 6) {
            return getMorningBeforeEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
        }
        return 0;
    }

    private static long getJobSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd,
                                      LocalDateTime startInterval, LocalDateTime endInterval, Map<String, Integer> dimTime) {
        if (!isWeekend(dateTimeEnd.getDayOfWeek()) && !isHoliday(dateTimeStart, dimTime)) {
            return getDaySeconds(dateTimeStart, dateTimeEnd, startInterval, endInterval);
        }
        return 0;
    }

    private static Pair<Long, DayPeriodEnum> getHomeSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd,
                                                            LocalDateTime dt1, LocalDateTime dt2, LocalDateTime dt3, LocalDateTime dt4,
                                                            Map<String, Integer> dimTime) {
        long homeResult = 0;
        DayPeriodEnum dayPeriodEnum = DayPeriodEnum.NONE;
        if (isHoliday(dateTimeStart, dimTime)) {
            return Pair.of(homeResult, DayPeriodEnum.NONE);
        }
        if (!isWeekend(dateTimeStart.getDayOfWeek())) {
            long earlyMorning = getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
            long night = getNightSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
            homeResult += earlyMorning;
            homeResult += night;
            return Pair.of(homeResult, resolveDayPeriodEnum(earlyMorning, night));
        } else {
            if (dateTimeStart.getDayOfWeek() == 6) {
                homeResult += getEarlyMorningSeconds(dateTimeStart, dateTimeEnd, dt1, dt2);
                if (homeResult > 0) {
                    dayPeriodEnum = DayPeriodEnum.EARLY_MORNING;
                }
                return Pair.of(homeResult, dayPeriodEnum);
            } else {
                homeResult += getNightSeconds(dateTimeStart, dateTimeEnd, dt3, dt4);
                if (homeResult > 0) {
                    dayPeriodEnum = DayPeriodEnum.NIGHT;
                }
                return Pair.of(homeResult, dayPeriodEnum);
            }
        }
    }

    private static DayPeriodEnum resolveDayPeriodEnum(long earlyMorning, long night) {
        if (earlyMorning > 0 && night > 0) {
            return DayPeriodEnum.BOTH;
        }
        if (earlyMorning > 0) {
            return DayPeriodEnum.EARLY_MORNING;
        }
        if (night > 0) {
            return DayPeriodEnum.NIGHT;
        }
        return DayPeriodEnum.NONE;
    }

    private static long getNightSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
        if (isFullPeriod(dateTimeStart, dateTimeEnd, startInterval, endInterval)) {
            return getFullPeriodSeconds(startInterval, endInterval);
        }
        if (isNight(dateTimeStart.getHourOfDay())) {
            if (isNight(dateTimeEnd.getHourOfDay())) {
                return Seconds.secondsBetween(dateTimeStart, dateTimeEnd).getSeconds();
            } else {
                return Seconds.secondsBetween(dateTimeStart, endInterval).getSeconds();
            }
        } else if (isNight(dateTimeEnd.getHourOfDay())) {
            return Seconds.secondsBetween(startInterval, dateTimeEnd).getSeconds();
        }
        return 0;
    }

    private static long getEveningSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd,
                                          LocalDateTime startInterval, LocalDateTime endInterval, Map<String, Integer> dimTime) {
        if (!isWeekend(dateTimeStart.getDayOfWeek()) && !isHoliday(dateTimeStart, dimTime)) {
            return getEveningSeconds(dateTimeStart, dateTimeEnd, startInterval, endInterval);
        }
        return 0;
    }

    private static long getEveningSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
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

    private static long getDaySeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
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

    private static long getEarlyMorningSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
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

    private static long getMorningSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd,
                                          LocalDateTime startInterval, LocalDateTime endInterval, Map<String, Integer> dimTime) {
        if (!isWeekend(dateTimeStart.getDayOfWeek()) && !isHoliday(dateTimeStart, dimTime)) {
            return getMorningSeconds(dateTimeStart, dateTimeEnd, startInterval, endInterval);
        }
        return 0;
    }

    private static long getMorningSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
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

    private static long getEarlyMorningBeforeNightSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
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

    private static long getMorningBeforeEarlyMorningSeconds(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
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

    private static long getFullPeriodSeconds(LocalDateTime dt1, LocalDateTime dt2) {
        return Seconds.secondsBetween(dt1, dt2.minusMillis(1)).getSeconds();
    }

    private static boolean isFullPeriod(LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, LocalDateTime startInterval, LocalDateTime endInterval) {
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

    private static boolean isHoliday(LocalDateTime dateTime, Map<String, Integer> dimTime) {
        Integer index = dimTime.get(dateTime.toString(DimTime.Constants.DATE_FORMATTER_OUTPUT));
        return index != null && index == 1;
    }

    private enum DayPeriodEnum {
        NIGHT, EARLY_MORNING, NONE, BOTH
    }

}
