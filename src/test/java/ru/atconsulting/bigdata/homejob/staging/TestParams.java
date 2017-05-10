package ru.atconsulting.bigdata.homejob.staging;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class TestParams {
    private static final String PATH_AGG_GEO_LAYER = "stage1/input/geo_layer.csv";
    private static final String PATH_AGG_GEO_LAYER_2 = "version5/input/agg_geo_layer.csv";
    private static final String PATH_STAGE_1_OUT = "stage1/output/stage1_version1.txt";
    private static final String PATH_STAGE_1_OUT_UNIT = "stage2/input/stage1_unit.csv";
    private static final String PATH_STAGE_2_OUT = "stage2/output/stage2_version1.txt";
    private static final String PATH_STAGE_2_OUT_2 = "stage2/output/stage2_version1.csv";
    private static final String PATH_STAGE_3_OUT = "stage3/output/stage3_version1.txt";
    private static final String PATH_DIM_TIME_2  = "stage2/dim/dim_time.csv";
    private static final String PATH_DIM_TIME_2_2  = "version5/input/dim_time.csv";
    private static final String PATH_DIM_GRID_ALL_3  = "stage5/dim/dim_bs_grid_all.csv";
    private static final String PATH_DIM_GRID_ALL_3_2  = "version5/input/dim_bs_grid_all.csv";


    public enum PATH_ENUM {
        AGG_GEO_LAYER,
        AGG_GEO_LAYER_2,
        STAGE_1_OUT,
        STAGE_1_OUT_UNIT,
        STAGE_2_OUT,
        STAGE_2_OUT_2,
        DIM_TIME_2,
        DIM_TIME_2_2,
        DIM_GRID_ALL_3,
        DIM_GRID_ALL_3_2,
        STAGE_3_OUT
    }

    public static Path getPath(PATH_ENUM pathEnum){
        try {
            switch (pathEnum) {
                case AGG_GEO_LAYER: {
                    return getPathWithJar(PATH_AGG_GEO_LAYER);
                }
                case AGG_GEO_LAYER_2: {
                    return getPathWithJar(PATH_AGG_GEO_LAYER_2);
                }
                case STAGE_1_OUT: {
                    return getPathWithJar(PATH_STAGE_1_OUT);
                }
                case STAGE_1_OUT_UNIT:{
                    return getPathWithJar(PATH_STAGE_1_OUT_UNIT);
                }
                case STAGE_2_OUT:{
                    return getPathWithJar(PATH_STAGE_2_OUT);
                }
                case STAGE_2_OUT_2:{
                    return getPathWithJar(PATH_STAGE_2_OUT_2);
                }
                case STAGE_3_OUT:{
                    return getPathWithJar(PATH_STAGE_3_OUT);
                }
                case DIM_TIME_2: {
                    return getPathWithJar(PATH_DIM_TIME_2);
                }
                case DIM_TIME_2_2: {
                    return getPathWithJar(PATH_DIM_TIME_2_2);
                }
                case DIM_GRID_ALL_3:{
                    return getPathWithJar(PATH_DIM_GRID_ALL_3);
                }
                case DIM_GRID_ALL_3_2:{
                    return getPathWithJar(PATH_DIM_GRID_ALL_3_2);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Can't find this path");
    }

    private static Path getPathWithJar(String fromResourcesStr) throws URISyntaxException {
        return Paths.get(TestParams.class.getResource("/" + fromResourcesStr).toURI());
    }


}
