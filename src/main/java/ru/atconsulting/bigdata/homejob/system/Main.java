package ru.atconsulting.bigdata.homejob.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * Created by NSkovpin on 08.06.2015.
 */
public class Main extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Main.class);
    private ClusterProperties clusterProperties;

    public static void main(String[] args) throws Exception {
       int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        clusterProperties = new ClusterProperties(conf);
        LOG.info("!Started main driver!");

        int result = 0;
//        switch (clusterProperties.STAGE) {
//            case 1:
//                result = ToolRunner.run(conf, new DSingleMonthNew(), strings);
//                break;
//            case 2:
//                result = ToolRunner.run(conf, new DAllMonths(), strings);
//                break;
//            case 3:
//                result = ToolRunner.run(conf, new DGroup(), strings);
//                break;
//            case 4:
//                result = ToolRunner.run(conf, new DFctImei(), strings);
//                break;
//            case 5:
//                result = ToolRunner.run(conf, new DBankScor(), strings);
//                break;
//            case 6:
//                result = ToolRunner.run(conf, new DBToB(), strings);
//                break;
//            case 7:
//                result = ToolRunner.run(conf, new DCramerAndBsPosition(), strings);
//                break;
//            case 8:
//                result = ToolRunner.run(conf, new DHomeJob(), strings);
//                break;
//            case 9:
//                result = ToolRunner.run(conf, new DResult(), strings);
//                break;
//            default:
//                LOG.info("Wrong stage number!");
//                break;
//        }
        return result;
    }
}
