package ru.atconsulting.bigdata.homejob.system.util.date;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 17.04.2017.
 */
@Getter
@Setter
public class TimeSummary {

    private boolean autoIncrement;

    private long home;
    private long job;
    private long evening;
    private long morning;
    private long weekend;
    private long weekendDay;
    private long weekendNight;

    private long homeCount;
    private long jobCount;
    private long eveningCount;
    private long morningCount;
    private long weekendCount;
    private long weekendDayCount;
    private long weekendNightCount;

    public TimeSummary() {
        this.autoIncrement = true;
    }

    public TimeSummary(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public void incrementHome(long home) {
        if (home > 0 && autoIncrement) {
            this.incrementHomeCount();
        }
        this.home += home;
    }

    public void incrementHome(long home, boolean withoutIncrement) {
        if (home > 0 && autoIncrement && !withoutIncrement) {
            this.incrementHomeCount();
        }
        this.home += home;
    }

    public void incrementJob(long job) {
        if (job > 0 && autoIncrement) {
            this.incrementJobCount();
        }
        this.job += job;
    }

    public void incrementEvening(long evening) {
        if (evening > 0 && autoIncrement) {
            this.incrementEveningCount();
        }
        this.evening += evening;
    }

    public void incrementMorning(long morning) {
        if (morning > 0 && autoIncrement) {
            this.incrementMorningCount();
        }
        this.morning += morning;
    }

    public void incrementWeekend(long weekend) {
        if (weekend > 0 && autoIncrement) {
            this.incrementWeekendCount();
        }
        this.weekend += weekend;
    }

    public void incrementWeekendDay(long weekendDay) {
        if (weekendDay > 0 && autoIncrement) {
            this.incrementWeekendDayCount();
        }
        this.weekendDay += weekendDay;
    }

    public void incrementWeekendNight(long weekendNight) {
        if (weekendNight > 0 && autoIncrement) {
            this.incrementWeekendNightCount();
        }
        this.weekendNight += weekendNight;
    }

    public void incrementWeekendNight(long weekendNight, boolean withoutIncrement) {
        if (weekendNight > 0 && autoIncrement && !withoutIncrement) {
            this.incrementWeekendNightCount();
        }
        this.weekendNight += weekendNight;
    }

    public void incrementHomeCount(long count) {
        this.homeCount +=count;
    }

    public void incrementJobCount(long count) {
        this.jobCount+=count;
    }

    public void incrementEveningCount(long count) {
        this.eveningCount+=count;
    }

    public void incrementMorningCount(long count) {
        this.morningCount+=count;
    }

    public void incrementWeekendCount(long count) {
        this.weekendCount+=count;
    }

    public void incrementWeekendDayCount(long count) {
        this.weekendDayCount+=count;
    }

    public void incrementWeekendNightCount(long count) {
        this.weekendNightCount+=count;
    }

    private void incrementHomeCount() {
        this.homeCount++;
    }

    private void incrementJobCount() {
        this.jobCount++;
    }

    private void incrementEveningCount() {
        this.eveningCount++;
    }

    private void incrementMorningCount() {
        this.morningCount++;
    }

    private void incrementWeekendCount() {
        this.weekendCount++;
    }

    private void incrementWeekendDayCount() {
        this.weekendDayCount++;
    }

    private void incrementWeekendNightCount() {
        this.weekendNightCount++;
    }

}
