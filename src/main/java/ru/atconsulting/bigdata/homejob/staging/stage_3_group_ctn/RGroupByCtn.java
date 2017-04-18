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
        clusterProperties =  new ClusterProperties(context.getConfiguration());
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
            String[] valueRow = value.toString().split(GeoLayer.Constant.FIELD_DELIMITER);

            if(imsi.isEmpty()){
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

    }


}
