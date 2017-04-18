package ru.atconsulting.bigdata.homejob.staging.stage_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;
import ru.at_consulting.bigdata.secondary_sort.CompositeKeyComparator;
import ru.at_consulting.bigdata.secondary_sort.GroupingKeyComparator;
import ru.atconsulting.bigdata.homejob.staging.TestParams;
import ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi.MLoadGeo;
import ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi.RFirstImsi;
import ru.atconsulting.bigdata.homejob.system.util.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 17.04.2017.
 */
@SuppressWarnings("unchecked")
public class Stage1UniqueTest {

    private static MultipleInputsMapReduceDriver<ComparedKey, Text, NullWritable, Text> mapReduceDriver;
    private MLoadGeo mLoadGeo = new MLoadGeo();
    private RFirstImsi rFirstImsi = new RFirstImsi();

    @Before
    public void setup() {
        Configuration conf = new Configuration();

        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver();
        mapReduceDriver.setReducer(rFirstImsi);
        mapReduceDriver.addMapper(mLoadGeo);
        mapReduceDriver.setConfiguration(conf);

        mapReduceDriver.setKeyGroupingComparator(new GroupingKeyComparator());
        mapReduceDriver.setKeyOrderComparator(new CompositeKeyComparator());

    }

    @Test
    public void testStage() throws IOException {

        List<String> geoLayerList = new ArrayList<String>();
        List<String> resultList = new ArrayList<String>();

        geoLayerList.addAll(Files.readAllLines(TestParams.getPath(TestParams.PATH_ENUM.AGG_GEO_LAYER), Charset.defaultCharset()));
        resultList.addAll(Files.readAllLines(TestParams.getPath(TestParams.PATH_ENUM.STAGE_1_OUT), Charset.defaultCharset()));

        // GEO_LAYER
        for (String row : geoLayerList) {
            mapReduceDriver.addInput(this.mLoadGeo, NullWritable.get(), new Text(row));
        }

        // RESULT
        for (String row : resultList) {
            mapReduceDriver.addOutput(NullWritable.get(), new Text(row));
        }

        mapReduceDriver.runTest(false);
    }

}
