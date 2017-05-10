package ru.atconsulting.bigdata.homejob.staging.stage_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import ru.at_consulting.bigdata.secondary_sort.CompositeKeyComparator;
import ru.at_consulting.bigdata.secondary_sort.GroupingKeyComparator;
import ru.atconsulting.bigdata.homejob.staging.TestParams;
import ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals.MDivideIntervals;
import ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals.RSumIntervals;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.MAddGrid;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.RGroupByCtn;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 18.04.2017.
 */
public class Stage3GroupTest {

    private static MultipleInputsMapReduceDriver<Text, Text, NullWritable, Text> mapReduceDriver;
    private MAddGrid mapperAddGrid = new MAddGrid();
    private RGroupByCtn reducerGroupByCtn = new RGroupByCtn();

    @Before
    public void setup() {
        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver();
        mapReduceDriver.setReducer(reducerGroupByCtn);
        mapReduceDriver.addMapper(mapperAddGrid);
        Configuration configuration = mapReduceDriver.getConfiguration();
        configuration.set(ClusterProperties.PARAM_NAMES.HDFS_DIM_GRID_ALL_PATH.name(), TestParams.getPath(TestParams.PATH_ENUM.DIM_GRID_ALL_3_2).toString());
        configuration.set(ClusterProperties.PARAM_NAMES.TIME_KEY.name(),"201612");

        mapReduceDriver.addCacheFile(new Path(TestParams.getPath(TestParams.PATH_ENUM.DIM_GRID_ALL_3_2).toString()).toUri());
    }

    @Test
    public void testStage() throws IOException {

        List<String> stage2Output = new ArrayList<>();
        List<String> resultList = new ArrayList<>();

        stage2Output.addAll(Files.readAllLines(TestParams.getPath(TestParams.PATH_ENUM.STAGE_2_OUT), Charset.defaultCharset()));
        resultList.addAll(Files.readAllLines(TestParams.getPath(TestParams.PATH_ENUM.STAGE_3_OUT), Charset.defaultCharset()));

        // STAGE_1
        for (String row : stage2Output) {
            mapReduceDriver.addInput(this.mapperAddGrid, NullWritable.get(), new Text(row));
        }

        // RESULT
        for (String row : resultList) {
            mapReduceDriver.addOutput(NullWritable.get(), new Text(row));
        }

        mapReduceDriver.runTest(false);
    }

}
