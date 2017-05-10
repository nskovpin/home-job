package ru.atconsulting.bigdata.homejob.staging.stage_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.joda.time.DateTime;
import org.joda.time.Months;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.junit.Before;
import org.junit.Test;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;
import ru.at_consulting.bigdata.secondary_sort.CompositeKeyComparator;
import ru.at_consulting.bigdata.secondary_sort.GroupingKeyComparator;
import ru.atconsulting.bigdata.homejob.staging.TestParams;
import ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi.MLoadGeo;
import ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi.RFirstImsi;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;
import ru.atconsulting.bigdata.homejob.system.util.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class Stage1UniqueTest {

    private static MultipleInputsMapReduceDriver<ComparedKey, Text, NullWritable, Text> mapReduceDriver;
    private MLoadGeo mLoadGeo = new MLoadGeo();
    private RFirstImsi rFirstImsi = new RFirstImsi();

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver();
        mapReduceDriver.setReducer(rFirstImsi);
        mapReduceDriver.addMapper(mLoadGeo);
        Configuration  configuration = mapReduceDriver.getConfiguration();
        configuration.set(ClusterProperties.PARAM_NAMES.TIME_KEY.name(),"201612");

        mapReduceDriver.setKeyGroupingComparator(new GroupingKeyComparator());
        mapReduceDriver.setKeyOrderComparator(new CompositeKeyComparator());

    }

    @Test
    public void testStage() throws IOException {

        List<String> geoLayerList = new ArrayList<>();
        List<String> resultList = new ArrayList<>();

        geoLayerList.addAll(Files.readAllLines(TestParams.getPath(TestParams.PATH_ENUM.AGG_GEO_LAYER_2), Charset.defaultCharset()));
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

    @Test
    public void loadTest(){
        DateTime startDate = DateTime.now();
        DateTime endDate = startDate;
        String basePath = "user/tech/geo_layer/";
        String formatter = "YYYYMM";
        BaseSingleFieldPeriod step = Months.ONE;

        for(; !startDate.isAfter(endDate); startDate = startDate.plus(step)) {
            String s = basePath + startDate.toString(formatter);
            System.out.println("Loaded path:\t" + s);
            assert true;
        }
    }

}
