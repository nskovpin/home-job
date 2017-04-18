package ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi.RFirstImsi;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;
import ru.atconsulting.bigdata.homejob.system.pojo.DimTime;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;
import ru.atconsulting.bigdata.homejob.system.pojo.Tower;
import ru.atconsulting.bigdata.homejob.system.util.date.DateIntervalMaker;
import ru.atconsulting.bigdata.homejob.system.util.date.DateTerminator;
import ru.atconsulting.bigdata.homejob.system.util.date.TimeSummary;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class MDivideIntervals extends Mapper<WritableComparable, Text, Text, Text> {
    private static final Text KEY = new Text();
    private static final Text VALUE = new Text();
    private Map<String, Integer> dimTimeMap;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        ClusterProperties clusterProperties = new ClusterProperties(context.getConfiguration());
        URI[] files = context.getCacheFiles();
        for (URI file : files) {
            Path path = new Path(file);
            this.dimTimeMap = DimTime.loadDimTimeMap(path.toString(), clusterProperties.getTimeKey());
        }
        if (dimTimeMap.size() == 0) {
            throw new RuntimeException(">>>DimTime size is 0");
        }
    }


    @Override
    protected void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(GeoLayer.Constant.FIELD_DELIMITER, -1);
        if (row.length != RFirstImsi.OutputValue.LENGTH) {
            context.getCounter(GeoLayer.Counter.WRONG_COLUMN_LENGTH);
            return;
        }

        String ctn = row[RFirstImsi.OutputValue.INDEX_CTN];
        String imsi = row[RFirstImsi.OutputValue.INDEX_IMSI];
        String cellList = row[RFirstImsi.OutputValue.INDEX_CELL_LIST];
        DateTime startIntervalDate = DateTime.parse(row[RFirstImsi.OutputValue.INDEX_START_INTERVAL], GeoLayer.Constant.INTERVAL_FORMATTER);
        DateTime endIntervalDate = DateTime.parse(row[RFirstImsi.OutputValue.INDEX_END_INTERVAL], GeoLayer.Constant.INTERVAL_FORMATTER);

        List<DateIntervalMaker.GeoInterval> intervals = DateIntervalMaker.tryToMakeDayIntervals(startIntervalDate, endIntervalDate);
        TimeSummary timeSummary = DateTerminator.getTimeSummary(intervals, dimTimeMap);

        List<Tower> towerList = GeoLayer.divideCellList(cellList);

        for (Tower tower : towerList) {
            KEY.set(ctn + GeoLayer.Constant.FIELD_DELIMITER +
                    imsi + GeoLayer.Constant.FIELD_DELIMITER +
                    tower.getLac() + GeoLayer.Constant.FIELD_DELIMITER +
                    tower.getCellId());

            VALUE.set(timeSummary.getHome() + GeoLayer.Constant.FIELD_DELIMITER +
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
                    cellList);

            context.write(KEY, VALUE);
        }

    }


    public enum OutputKey {
        CTN,
        IMSI,
        LAC,
        CELL_ID;
    }

    public enum OutputValue {
        INDEX_HOME,
        INDEX_JOB,
        INDEX_EVENING,
        INDEX_MORNING,
        INDEX_WEEKEND,
        INDEX_WEEKEND_DAY,
        INDEX_WEEKEND_NIGHT,
        INDEX_HOME_COUNT,
        INDEX_JOB_COUNT,
        INDEX_EVENING_COUNT,
        INDEX_MORNING_COUNT,
        INDEX_WEEKEND_COUNT,
        INDEX_WEEKEND_DAY_COUNT,
        INDEX_WEEKEND_NIGHT_COUNT,
        INDEX_CELL_LIST;
    }


}
