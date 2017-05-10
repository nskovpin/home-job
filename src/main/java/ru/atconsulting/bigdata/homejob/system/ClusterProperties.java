package ru.atconsulting.bigdata.homejob.system;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.File;

/**
 * Created by NSkovpin on 06.03.2017.
 */
@Getter
@ToString
public final class ClusterProperties {
    private static final String STG = "stg";
    private static final String STAGING = "staging";
    public static final String EMPTY = "";
    public static final String EMPTY_LOCATION_NOT_FOUND = "Location_not_found";
    public static final String WORK = "work";
    public static final String DATA = "data";
    public static final String TIME_KEY_PATTERN = "YYYYMM";

    private final String hdfsGeoLayerPath;
    private final String hdfsDimTimePath;
    private final String hdfsDimGridAllPath;

    private final String projectName;
    private final String hdfsOutputWorkDir;
    private final String hdfsOutputDataDir;

    private final String hdfsStgUniqueImsi;
    private final String hdfsStgIntervals;
    private final String hdfsResult;

    private final LocalDateTime timeKey;

    private final int stage;

    public enum PARAM_NAMES {
        PROJECT_NAME,
        HDFS_GEO_LAYER_PATH,
        HDFS_DIM_TIME_PATH,
        HDFS_DIM_GRID_ALL_PATH,
        WORKING_DIR,
        OUTPUT_DIR,
        TIME_KEY,
        STAGE
    }

    public ClusterProperties(Configuration configuration) {
        this.projectName = configuration.get(PARAM_NAMES.PROJECT_NAME.name());
        this.stage = Integer.parseInt(configuration.get(PARAM_NAMES.STAGE.name(), "0"));

        this.hdfsGeoLayerPath = configuration.get(PARAM_NAMES.HDFS_GEO_LAYER_PATH.name());
        this.hdfsDimGridAllPath = configuration.get(PARAM_NAMES.HDFS_DIM_GRID_ALL_PATH.name());
        this.hdfsDimTimePath = configuration.get(PARAM_NAMES.HDFS_DIM_TIME_PATH.name());

        this.hdfsOutputWorkDir = configuration.get(PARAM_NAMES.WORKING_DIR.name());
        this.hdfsOutputDataDir = configuration.get(PARAM_NAMES.OUTPUT_DIR.name());

        this.hdfsStgUniqueImsi = hdfsOutputDataDir + File.separator + STAGING + File.separator + STG + "_unique_imsi";
        this.hdfsStgIntervals = hdfsOutputDataDir + File.separator + STAGING + File.separator + STG + "_intervals";

        this.timeKey = LocalDateTime.parse(configuration.get(PARAM_NAMES.TIME_KEY.name()), DateTimeFormat.forPattern(TIME_KEY_PATTERN));
        this.hdfsResult = hdfsOutputDataDir + File.separator + timeKey.toString(TIME_KEY_PATTERN);

    }
}
