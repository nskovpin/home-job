package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair.Pair;
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

        TopHolder homeHolder = new TopHolder(false, true);
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

        Pair<String, String> homeTop2 = getHomePeriodAndOther(homeHolder.getTop2Value(1));

        VALUE.set(ctn + GeoLayer.Constant.FIELD_DELIMITER +
                imsi + GeoLayer.Constant.FIELD_DELIMITER +
                clusterProperties.getTimeKey().toString("YYYY-MM-dd") + GeoLayer.Constant.FIELD_DELIMITER +
                homeHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                jobHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                eveningHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                morningHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                weekendHolder.getTop1Value(1) + GeoLayer.Constant.FIELD_DELIMITER +
                homeTop2.getValue() + GeoLayer.Constant.FIELD_DELIMITER +
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
                homeTop2.getKey() + GeoLayer.Constant.FIELD_DELIMITER +
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

    private Pair<String, String> getHomePeriodAndOther(String homeString) {
        String[] homeArray = homeString.split(GeoLayer.Constant.FIELD_DELIMITER, -1);
        Pair<String, String> pair = new Pair<>();
        pair.setKey(homeArray[TopHolder.HOME_INDEXIES.PERIOD.ordinal()]);
        pair.setValue(homeArray[TopHolder.HOME_INDEXIES.LAC.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                homeArray[TopHolder.HOME_INDEXIES.CELL.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                homeArray[TopHolder.HOME_INDEXIES.LAT.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                homeArray[TopHolder.HOME_INDEXIES.LON.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                homeArray[TopHolder.HOME_INDEXIES.ADDRESS.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                homeArray[TopHolder.HOME_INDEXIES.CELL_LIST.ordinal()]
        );
        return pair;
    }

    private enum OutputValue {
        ctn ,
        imsi ,
        month ,
        home_top_1_lac ,
        home_top_1_cell_id ,
        home_top_1_latitude ,
        home_top_1_longitude ,
        home_top_1_address_norm ,
        home_top_1_period_count ,
        home_top_1_cell_list ,
        job_top_1_lac ,
        job_top_1_cell_id ,
        job_top_1_latitude ,
        job_top_1_longitude ,
        job_top_1_address_norm ,
        job_top_1_cell_list ,
        evening_lac ,
        evening_cell_id ,
        evening_latitude ,
        evening_longitude ,
        evening_address_norm ,
        evening_cell_list ,
        morning_lac ,
        morning_cell_id ,
        morning_latitude ,
        morning_longitude ,
        morning_address_norm ,
        morning_cell_list ,
        weekend_lac ,
        weekend_cell_id ,
        weekend_latitude ,
        weekend_longitude ,
        weekend_address_norm ,
        weekend_cell_list ,
        home_top_2_lac ,
        home_top_2_cell_id ,
        home_top_2_latitude ,
        home_top_2_longitude ,
        home_top_2_address_norm ,
        home_top_2_cell_list ,
        job_top_2_lac ,
        job_top_2_cell_id ,
        job_top_2_latitude ,
        job_top_2_longitude ,
        job_top_2_address_norm ,
        job_top_2_cell_list ,
        weekend_day_top_1_lac ,
        weekend_day_top_1_cell_id ,
        weekend_day_top_1_latitude ,
        weekend_day_top_1_longitude ,
        weekend_day_top_1_address_norm ,
        weekend_day_top_1_cell_list ,
        weekend_day_top_2_lac ,
        weekend_day_top_2_cell_id ,
        weekend_day_top_2_latitude ,
        weekend_day_top_2_longitude ,
        weekend_day_top_2_address_norm ,
        weekend_day_top_2_cell_list ,
        weekend_night_top_1_lac ,
        weekend_night_top_1_cell_id ,
        weekend_night_top_1_latitude ,
        weekend_night_top_1_longitude ,
        weekend_night_top_1_address_norm ,
        weekend_night_top_1_cell_list ,
        weekend_night_top_2_lac ,
        weekend_night_top_2_cell_id ,
        weekend_night_top_2_latitude ,
        weekend_night_top_2_longitude ,
        weekend_night_top_2_address_norm ,
        weekend_night_top_2_cell_list ,
        home_top_1_hours ,
        home_top_1_pc ,
        home_top_1_branch_id ,
        home_top_1_city_id ,
        home_top_1_grid_list ,
        job_top_1_period_count ,
        job_top_1_hours ,
        job_top_1_pc ,
        job_top_1_branch_id ,
        job_top_1_city_id ,
        job_top_1_grid_list ,
        evening_period_count ,
        evening_hours ,
        evening_pc ,
        evening_branch_id ,
        evening_city_id ,
        evening_grid_list ,
        morning_period_count ,
        morning_hours ,
        morning_pc ,
        morning_branch_id ,
        morning_city_id ,
        morning_grid_list ,
        weekend_period_count ,
        weekend_hours ,
        weekend_pc ,
        weekend_branch_id ,
        weekend_city_id ,
        weekend_grid_list ,
        home_top_2_period_count ,
        home_top_2_hours ,
        home_top_2_pc ,
        home_top_2_branch_id ,
        home_top_2_city_id ,
        home_top_2_grid_list ,
        job_top_2_period_count ,
        job_top_2_hours ,
        job_top_2_pc ,
        job_top_2_branch_id ,
        job_top_2_city_id ,
        job_top_2_grid_list ,
        weekend_day_top_1_period_count ,
        weekend_day_top_1_hours ,
        weekend_day_top_1_pc ,
        weekend_day_top_1_branch_id ,
        weekend_day_top_1_city_id ,
        weekend_day_top_1_grid_list ,
        weekend_day_top_2_period_count ,
        weekend_day_top_2_hours ,
        weekend_day_top_2_pc ,
        weekend_day_top_2_branch_id ,
        weekend_day_top_2_city_id ,
        weekend_day_top_2_grid_list ,
        weekend_night_top_1_period_count ,
        weekend_night_top_1_hours ,
        weekend_night_top_1_pc ,
        weekend_night_top_1_branch_id ,
        weekend_night_top_1_city_id ,
        weekend_night_top_1_grid_list ,
        weekend_night_top_2_period_count ,
        weekend_night_top_2_hours ,
        weekend_night_top_2_pc ,
        weekend_night_top_2_branch_id ,
        weekend_night_top_2_city_id ,
        weekend_night_top_2_grid_list;
    }

}
