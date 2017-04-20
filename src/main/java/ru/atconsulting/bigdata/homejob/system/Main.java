package ru.atconsulting.bigdata.homejob.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import ru.atconsulting.bigdata.homejob.staging.stage_1_unique_imsi.DDriver1;
import ru.atconsulting.bigdata.homejob.staging.stage_2_calculate_intervals.DDriver2;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.DDriver3;


/**
 * Created by NSkovpin on 08.06.2015.
 */
public class Main extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
       int res = ToolRunner.run(new Configuration(), new Main(), args);
       System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        ClusterProperties clusterProperties = new ClusterProperties(conf);
        LOG.info("Started main driver!");

        int result = 0;
        switch (clusterProperties.getStage()) {
            case 0: {
                int stage1Result = ToolRunner.run(conf, new DDriver1(), strings);
                int stage2Result = ToolRunner.run(conf, new DDriver2(), strings);
                int stage3Result = ToolRunner.run(conf, new DDriver3(), strings);

                result = ((stage1Result == 0) && (stage2Result == 0) && (stage3Result == 0)) ? 0 : 1;
                break;
            }
            case 1:
                result = ToolRunner.run(conf, new DDriver1(), strings);
                break;
            case 2:
                result = ToolRunner.run(conf, new DDriver2(), strings);
                break;
            case 3:
                result = ToolRunner.run(conf, new DDriver3(), strings);
                break;
            default:
                LOG.info("Wrong stage number!");
                break;
        }
        return result;
    }
}
