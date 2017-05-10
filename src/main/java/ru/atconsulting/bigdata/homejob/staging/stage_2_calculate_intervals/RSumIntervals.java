package ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;
import ru.atconsulting.bigdata.homejob.system.util.date.TimeSummary;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class RSumIntervals extends Reducer<Text, Text, NullWritable, Text> {
    private static final NullWritable KEY = NullWritable.get();
    private static final Text VALUE = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TimeSummary timeSummary = new TimeSummary(false);
        Set<String> cellList = new HashSet<>();
        for (Text value : values) {
            String valueRow[] = value.toString().split(GeoLayer.Constant.FIELD_DELIMITER, -1);
            timeSummary.incrementHome(Long.parseLong(valueRow[MDivideIntervals.OutputValue.HOME.ordinal()]));
            timeSummary.incrementJob(Long.parseLong(valueRow[MDivideIntervals.OutputValue.JOB.ordinal()]));
            timeSummary.incrementEvening(Long.parseLong(valueRow[MDivideIntervals.OutputValue.EVENING.ordinal()]));
            timeSummary.incrementMorning(Long.parseLong(valueRow[MDivideIntervals.OutputValue.MORNING.ordinal()]));
            timeSummary.incrementWeekend(Long.parseLong(valueRow[MDivideIntervals.OutputValue.WEEKEND.ordinal()]));
            timeSummary.incrementWeekendDay(Long.parseLong(valueRow[MDivideIntervals.OutputValue.WEEKEND_DAY.ordinal()]));
            timeSummary.incrementWeekendNight(Long.parseLong(valueRow[MDivideIntervals.OutputValue.WEEKEND_NIGHT.ordinal()]));

            timeSummary.incrementHomeCount(Long.parseLong(valueRow[MDivideIntervals.OutputValue.HOME_COUNT.ordinal()]));
            timeSummary.incrementJobCount(Long.parseLong(valueRow[MDivideIntervals.OutputValue.JOB_COUNT.ordinal()]));
            timeSummary.incrementEveningCount(Long.parseLong(valueRow[MDivideIntervals.OutputValue.EVENING_COUNT.ordinal()]));
            timeSummary.incrementMorningCount(Long.parseLong(valueRow[MDivideIntervals.OutputValue.MORNING_COUNT.ordinal()]));
            timeSummary.incrementWeekendCount(Long.parseLong(valueRow[MDivideIntervals.OutputValue.WEEKEND_COUNT.ordinal()]));
            timeSummary.incrementWeekendDayCount(Long.parseLong(valueRow[MDivideIntervals.OutputValue.WEEKEND_DAY_COUNT.ordinal()]));
            timeSummary.incrementWeekendNightCount(Long.parseLong(valueRow[MDivideIntervals.OutputValue.WEEKEND_NIGHT_COUNT.ordinal()]));

            addTowers(cellList, valueRow[MDivideIntervals.OutputValue.CELL_LIST.ordinal()]);
        }
        Joiner joiner = Joiner.on(GeoLayer.Constant.CELL_LIST_DELIMITER);

        VALUE.set(key.toString() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getHome() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getJob() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getEvening() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getMorning() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getWeekend() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getWeekendDay() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getWeekendNight() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getHomeCount() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getJobCount() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getEveningCount() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getMorningCount() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getWeekendCount() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getWeekendDayCount() + GeoLayer.Constant.FIELD_DELIMITER +
                timeSummary.getWeekendNightCount() + GeoLayer.Constant.FIELD_DELIMITER +
                joiner.join(cellList)
        );

        context.write(KEY, VALUE);
    }

    private void addTowers(Set<String> setCells, String cellList) {
        String[] cellArray = cellList.split(GeoLayer.Constant.CELL_LIST_DELIMITER);
        Collections.addAll(setCells, cellArray);
    }

    public enum OutputKey {
        ;
    }

    public enum OutputValue {
        CTN,
        IMSI,
        LAC,
        CELL_ID,
        HOME,
        JOB,
        EVENING,
        MORNING,
        WEEKEND,
        WEEKEND_DAY,
        WEEKEND_NIGHT,
        HOME_COUNT,
        JOB_COUNT,
        EVENING_COUNT,
        MORNING_COUNT,
        WEEKEND_COUNT,
        WEEKEND_DAY_COUNT,
        WEEKEND_NIGHT_COUNT,
        CELL_LIST
    }
}
