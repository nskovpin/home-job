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
                firstImsi = row[MLoadGeo.OutputValue.INDEX_IMSI];
            }
            VALUE.set(ctn + GeoLayer.Constant.FIELD_DELIMITER +
                    firstImsi + GeoLayer.Constant.FIELD_DELIMITER +
                    row[MLoadGeo.OutputValue.INDEX_START_INTERVAL] + GeoLayer.Constant.FIELD_DELIMITER +
                    row[MLoadGeo.OutputValue.INDEX_END_INTERVAL] + GeoLayer.Constant.FIELD_DELIMITER +
                    row[MLoadGeo.OutputValue.INDEX_CELL_LIST]
            );
            context.write(KEY, VALUE);
        }

    }

    public static class OutputValue {
        public static final int LENGTH = 5;
        public static final int INDEX_CTN = 0;
        public static final int INDEX_IMSI = 1;
        public static final int INDEX_START_INTERVAL = 2;
        public static final int INDEX_END_INTERVAL = 3;
        public static final int INDEX_CELL_LIST = 4;
    }

}
