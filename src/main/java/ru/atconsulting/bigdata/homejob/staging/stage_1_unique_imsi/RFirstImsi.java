package ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;

import java.io.IOException;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class RFirstImsi extends Reducer<ComparedKey, Text, NullWritable, Text> {
    private static final NullWritable KEY = NullWritable.get();
    private static final Text VALUE = new Text();

    @Override
    protected void reduce(ComparedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String ctn = key.getKey().toString();
        String firstImsi = null;
        for (Text value : values) {
            String[] row = value.toString().split(GeoLayer.Constant.FIELD_DELIMITER);
            if (firstImsi == null) {
                firstImsi = row[MLoadGeo.OutputValue.IMSI.ordinal()];
            }
            VALUE.set(ctn + GeoLayer.Constant.FIELD_DELIMITER +
                    firstImsi + GeoLayer.Constant.FIELD_DELIMITER +
                    row[MLoadGeo.OutputValue.START_INTERVAL.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                    row[MLoadGeo.OutputValue.END_INTERVAL.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                    row[MLoadGeo.OutputValue.CELL_LIST.ordinal()]
            );
            context.write(KEY, VALUE);
            System.out.println(VALUE.toString());
        }

    }

    public enum OutputValue {
        CTN,
        IMSI,
        START_INTERVAL,
        END_INTERVAL,
        CELL_LIST;
    }

}
