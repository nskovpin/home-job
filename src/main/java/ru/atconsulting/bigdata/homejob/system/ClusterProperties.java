package ru.atconsulting.bigdata.homejob.system;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.File;

/**
 * Created by NSkovpin on 06.03.2017.
 */
@Getter
@Setter
@ToString
public class ClusterProperties {
    public static final String WORK = "work";
    public static final String DATA = "data";
    public static final String TIME_KEY_PATTERN = "YYYYMM";

    private String hdfsGeoLayerPath;
    private String hdfsDimTimePath;

    private String projectName;
    private String hdfsProjectDir;
    private String hdfsOutputWorkDir;
    private String hdfsOutputDataDir;
    private DateTime timeKey;

    public enum PARAM_NAMES {
        PROJECT_NAME,
        HDFS_GEO_LAYER_PATH,
        HDFS_DIM_TIME_PATH,
        HDFS_PROJECT_PATH,
        TIME_KEY,
    }

    public ClusterProperties(Configuration configuration){
        this.projectName = configuration.get(PARAM_NAMES.PROJECT_NAME.name());
        this.hdfsGeoLayerPath = configuration.get(PARAM_NAMES.HDFS_GEO_LAYER_PATH.name());
        this.hdfsProjectDir = configuration.get(PARAM_NAMES.HDFS_PROJECT_PATH.name());
        this.hdfsOutputWorkDir = hdfsProjectDir + File.separator + WORK;
        this.hdfsOutputDataDir = hdfsProjectDir + File.separator + DATA;
        this.timeKey = DateTime.parse(configuration.get(PARAM_NAMES.TIME_KEY.name()),
                DateTimeFormat.forPattern(TIME_KEY_PATTERN));
        this.hdfsDimTimePath = configuration.get(PARAM_NAMES.HDFS_DIM_TIME_PATH.name());
    }
}
