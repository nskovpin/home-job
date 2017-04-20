package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair.TopHolder;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;

import java.io.IOException;

/**
 * Created by NSkovpin on 18.04.2017.
 */
public class RGroupByCtn extends Reducer<Text, Text, NullWritable, Text> {
    private static final NullWritable KEY = NullWritable.get();
    private static final Text VALUE = new Text();
    private ClusterProperties clusterProperties;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        clusterProperties = new ClusterProperties(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String ctn = key.toString();
        String imsi = "";

        TopHolder homeHolder = new TopHolder();
        TopHolder jobHolder = new TopHolder();
        TopHolder eveningHolder = new TopHolder(true);
        TopHolder morningHolder = new TopHolder(true);
        TopHolder weekendHolder = new TopHolder(true);
        TopHolder weekendDayHolder = new TopHolder();
        TopHolder weekendNightHolder = new TopHolder();

        for (Text value : values) {
            String[] valueRow = value.toString().split(GeoLayer.Constant.FIELD_DELIMITER, -1);

            if (imsi.isEmpty()) {
                imsi = valueRow[MAddGrid.OutputValue.IMSI.ordinal()];
            }

            Long sumHome = Long.parseLong(valueRow[MAddGrid.OutputValue.HOME.ordinal()]);
            Long countHome = Long.parseLong(valueRow[MAddGrid.OutputValue.HOME_COUNT.ordinal()]);

            Long sumJob = Long.parseLong(valueRow[MAddGrid.OutputValue.JOB.ordinal()]);
            Long countJob = Long.parseLong(valueRow[MAddGrid.OutputValue.JOB_COUNT.ordinal()]);

            Long sumEvening = Long.parseLong(valueRow[MAddGrid.OutputValue.EVENING.ordinal()]);
            Long countEvening = Long.parseLong(valueRow[MAddGrid.OutputValue.EVENING_COUNT.ordinal()]);

            Long sumMorning = Long.parseLong(valueRow[MAddGrid.OutputValue.MORNING.ordinal()]);
            Long countMorning = Long.parseLong(valueRow[MAddGrid.OutputValue.MORNING_COUNT.ordinal()]);

            Long sumWeekend = Long.parseLong(valueRow[MAddGrid.OutputValue.WEEKEND.ordinal()]);
            Long countWeekend = Long.parseLong(valueRow[MAddGrid.OutputValue.WEEKEND_COUNT.ordinal()]);

            Long sumWeekendDay = Long.parseLong(valueRow[MAddGrid.OutputValue.WEEKEND_DAY.ordinal()]);
            Long countWeekendDay = Long.parseLong(valueRow[MAddGrid.OutputValue.WEEKEND_DAY_COUNT.ordinal()]);

            Long sumWeekendNight = Long.parseLong(valueRow[MAddGrid.OutputValue.WEEKEND_NIGHT.ordinal()]);
            Long countWeekendNight = Long.parseLong(valueRow[MAddGrid.OutputValue.WEEKEND_NIGHT_COUNT.ordinal()]);

            homeHolder.addToTop(sumHome, valueRow, sumHome, countHome);
            jobHolder.addToTop(sumJob, valueRow, sumJob, countJob);
            eveningHolder.addToTop(sumEvening, valueRow, sumEvening, countEvening);
            morningHolder.addToTop(sumMorning, valueRow, sumMorning, countMorning);
            weekendHolder.addToTop(sumWeekend, valueRow, sumWeekend, countWeekend);
            weekendDayHolder.addToTop(sumWeekendDay, valueRow, sumWeekendDay, countWeekendDay);
            weekendNightHolder.addToTop(sumWeekendNight, valueRow, sumWeekendNight, countWeekendNight);
        }

        VALUE.set(ctn + GeoLayer.Constant.FIELD_DELIMITER +
                imsi + GeoLayer.Constant.FIELD_DELIMITER +
                clusterProperties.getTimeKey().toString("YYYYMM") + GeoLayer.Constant.FIELD_DELIMITER +
                homeHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                jobHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                eveningHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                morningHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                homeHolder.getTop2Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                jobHolder.getTop2Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendDayHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendDayHolder.getTop2Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendNightHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendNightHolder.getTop2Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                homeHolder.getTop1Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                jobHolder.getTop1Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                eveningHolder.getTop1Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                morningHolder.getTop1Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendHolder.getTop1Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                homeHolder.getTop2Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                jobHolder.getTop2Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendDayHolder.getTop1Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendDayHolder.getTop2Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendNightHolder.getTop1Value(2) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendNightHolder.getTop2Value(2)
        );

        context.write(KEY, VALUE);
        context.getCounter(GeoLayer.Counter.GOOD_ROW).increment(1);
    }

    private enum OutputValue {
        CTN,
        IMSI,
        MONTH,
        HOME_TOP_1_LAC,
        HOME_TOP_1_CELL_ID,
        HOME_TOP_1_LATITUDE,
        HOME_TOP_1_LONGITUDE,
        HOME_TOP_1_ADDRESSSTRING_NORM,
        HOME_TOP_1_PERIOD_COUNT,
        HOME_TOP_1_CELL_LIST,
        JOB_TOP_1_LAC,
        JOB_TOP_1_CELL_ID,
        JOB_TOP_1_LATITUDE,
        JOB_TOP_1_LONGITUDE,
        JOB_TOP_1_ADDRESSSTRING_NORM,
        JOB_TOP_1_CELL_LIST,
        EVENING_LAC,
        EVENING_CELL_ID,
        EVENING_LATITUDE,
        EVENING_LONGITUDE,
        EVENING_ADDRESSSTRING_NORM,
        EVENING_CELL_LIST,
        MORNING_LAC,
        MORNING_CELL_ID,
        MORNING_LATITUDE,
        MORNING_LONGITUDE,
        MORNING_ADDRESSSTRING_NORM,
        MORNING_CELL_LIST,
        WEEKEND_LAC,
        WEEKEND_CELL_ID,
        WEEKEND_LATITUDE,
        WEEKEND_LONGITUDE,
        WEEKEND_ADDRESSSTRING_NORM,
        WEEKEND_CELL_LIST,
        HOME_TOP_2_LAC,
        HOME_TOP_2_CELL_ID,
        HOME_TOP_2_LATITUDE,
        HOME_TOP_2_LONGITUDE,
        HOME_TOP_2_ADDRESSSTRING_NORM,
        HOME_TOP_2_CELL_LIST,
        JOB_TOP_2_LAC,
        JOB_TOP_2_CELL_ID,
        JOB_TOP_2_LATITUDE,
        JOB_TOP_2_LONGITUDE,
        JOB_TOP_2_ADDRESSSTRING_NORM,
        JOB_TOP_2_CELL_LIST,
        WEEKEND_DAY_TOP_1_LAC,
        WEEKEND_DAY_TOP_1_CELL_ID,
        WEEKEND_DAY_TOP_1_LATITUDE,
        WEEKEND_DAY_TOP_1_LONGITUDE,
        WEEKEND_DAY_TOP_1_ADDRESSSTRING_NORM,
        WEEKEND_DAY_TOP_1_CELL_LIST,
        WEEKEND_DAY_TOP_2_LAC,
        WEEKEND_DAY_TOP_2_CELL_ID,
        WEEKEND_DAY_TOP_2_LATITUDE,
        WEEKEND_DAY_TOP_2_LONGITUDE,
        WEEKEND_DAY_TOP_2_ADDRESSSTRING_NORM,
        WEEKEND_DAY_TOP_2_CELL_LIST,
        WEEKEND_NIGHT_TOP_1_LAC,
        WEEKEND_NIGHT_TOP_1_CELL_ID,
        WEEKEND_NIGHT_TOP_1_LATITUDE,
        WEEKEND_NIGHT_TOP_1_LONGITUDE,
        WEEKEND_NIGHT_TOP_1_ADDRESSSTRING_NORM,
        WEEKEND_NIGHT_TOP_1_CELL_LIST,
        WEEKEND_NIGHT_TOP_2_LAC,
        WEEKEND_NIGHT_TOP_2_CELL_ID,
        WEEKEND_NIGHT_TOP_2_LATITUDE,
        WEEKEND_NIGHT_TOP_2_LONGITUDE,
        WEEKEND_NIGHT_TOP_2_ADDRESSSTRING_NORM,
        WEEKEND_NIGHT_TOP_2_CELL_LIST,
        HOME_TOP_1_HOURS,
        HOME_TOP_1_PC,
        HOME_TOP_1_BRANCH_ID,
        HOME_TOP_1_CITY_ID,
        HOME_TOP_1_GRID_LIST,
        JOB_TOP_1_PERIOD_COUNT,
        JOB_TOP_1_HOURS,
        JOB_TOP_1_PC,
        JOB_TOP_1_BRANCH_ID,
        JOB_TOP_1_CITY_ID,
        JOB_TOP_1_GRID_LIST,
        EVENING_PERIOD_COUNT,
        EVENING_HOURS,
        EVENING_PC,
        EVENING_BRANCH_ID,
        EVENING_CITY_ID,
        EVENING_GRID_LIST,
        MORNING_PERIOD_COUNT,
        MORNING_HOURS,
        MORNING_PC,
        MORNING_BRANCH_ID,
        MORNING_CITY_ID,
        MORNING_GRID_LIST,
        WEEKEND_PERIOD_COUNT,
        WEEKEND_HOURS,
        WEEKEND_PC,
        WEEKEND_BRANCH_ID,
        WEEKEND_CITY_ID,
        WEEKEND_GRID_LIST,
        HOME_TOP_2_PERIOD_COUNT,
        HOME_TOP_2_HOURS,
        HOME_TOP_2_PC,
        HOME_TOP_2_BRANCH_ID,
        HOME_TOP_2_CITY_ID,
        HOME_TOP_2_GRID_LIST,
        JOB_TOP_2_PERIOD_COUNT,
        JOB_TOP_2_HOURS,
        JOB_TOP_2_PC,
        JOB_TOP_2_BRANCH_ID,
        JOB_TOP_2_CITY_ID,
        JOB_TOP_2_GRID_LIST,
        WEEKEND_DAY_TOP_1_PERIOD_COUNT,
        WEEKEND_DAY_TOP_1_HOURS,
        WEEKEND_DAY_TOP_1_PC,
        WEEKEND_DAY_TOP_1_BRANCH_ID,
        WEEKEND_DAY_TOP_1_CITY_ID,
        WEEKEND_DAY_TOP_1_GRID_LIST,
        WEEKEND_DAY_TOP_2_PERIOD_COUNT,
        WEEKEND_DAY_TOP_2_HOURS,
        WEEKEND_DAY_TOP_2_PC,
        WEEKEND_DAY_TOP_2_BRANCH_ID,
        WEEKEND_DAY_TOP_2_CITY_ID,
        WEEKEND_DAY_TOP_2_GRID_LIST,
        WEEKEND_NIGHT_TOP_1_PERIOD_COUNT,
        WEEKEND_NIGHT_TOP_1_HOURS,
        WEEKEND_NIGHT_TOP_1_PC,
        WEEKEND_NIGHT_TOP_1_BRANCH_ID,
        WEEKEND_NIGHT_TOP_1_CITY_ID,
        WEEKEND_NIGHT_TOP_1_GRID_LIST,
        WEEKEND_NIGHT_TOP_2_PERIOD_COUNT,
        WEEKEND_NIGHT_TOP_2_HOURS,
        WEEKEND_NIGHT_TOP_2_PC,
        WEEKEND_NIGHT_TOP_2_BRANCH_ID,
        WEEKEND_NIGHT_TOP_2_CITY_ID,
        WEEKEND_NIGHT_TOP_2_GRID_LIST;
    }

}
