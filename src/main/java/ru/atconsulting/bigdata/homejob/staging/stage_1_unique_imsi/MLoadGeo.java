package ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;
import ru.atconsulting.bigdata.homejob.system.util.date.DateTerminator;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;

import java.io.IOException;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class MLoadGeo extends Mapper<WritableComparable, Text, ComparedKey, Text> {
    private static final ComparedKey KEY = new ComparedKey();
    private static final Text VALUE = new Text();
    private ClusterProperties properties;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        properties = new ClusterProperties(context.getConfiguration());
    }

    @Override
    protected void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(GeoLayer.Constant.FIELD_DELIMITER, -1);

        if (splitValue.length < GeoLayer.Constant.COLUMN_LENGTH) {
            context.getCounter(GeoLayer.Counter.WRONG_COLUMN_LENGTH).increment(1);
            return;
        }

        GeoLayer geoLayer = new GeoLayer(splitValue);
        if (geoLayer.isNotValid()) {
            context.getCounter(GeoLayer.Counter.WRONG_EMPTY_FIELD).increment(1);
            return;
        }

        geoLayer.toDateFields();

        if (!DateTerminator.isSameYearMonth(geoLayer.getTimeKeyDate(), properties.getTimeKey())) {
            context.getCounter(GeoLayer.Counter.WRONG_TIME_KEY).increment(1);
            return;
        }

        if (geoLayer.getInMetro().equals(GeoLayer.Constant.IN_METRO) || geoLayer.getCellList().equals(GeoLayer.Constant.CELL_LIST)) {
            context.getCounter(GeoLayer.Counter.WRONG_VALUE_FIELD).increment(1);
            return;
        }

        KEY.setComparedState(new LongWritable(-geoLayer.getTimeIntervalEndDate().toDateTime().getMillis())); //reverse order
        KEY.setKey(new Text(geoLayer.getCtn()));

        VALUE.set(new Text(geoLayer.getImsi() + GeoLayer.Constant.FIELD_DELIMITER +
                geoLayer.getTimeIntervalStartDate().toString(OutputValue.INTERVAL_FORMATTER) + GeoLayer.Constant.FIELD_DELIMITER +
                geoLayer.getTimeIntervalEndDate().toString(OutputValue.INTERVAL_FORMATTER) + GeoLayer.Constant.FIELD_DELIMITER +
                geoLayer.getCellList()
        ));

        context.write(KEY, VALUE);
        context.getCounter(GeoLayer.Counter.GOOD_ROW).increment(1);
    }

    public enum OutputKey {
        CTN;
    }

    public enum  OutputValue {
        IMSI,
        START_INTERVAL,
        END_INTERVAL,
        CELL_LIST;
        public static DateTimeFormatter INTERVAL_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    }


}
