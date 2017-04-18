package ru.atconsulting.bigdata.homejob.staging;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by NSkovpin on 17.04.2017.
 */
public class TestParams {
    private static final String PATH_AGG_GEO_LAYER = "stage1/input/geo_layer.txt";
    private static final String PATH_STAGE_1_OUT = "stage1/output/stage1.txt";
    private static final String PATH_DIM_TIME_2  = "stage2/dim/dimTime";

    public enum PATH_ENUM {
        AGG_GEO_LAYER,
        STAGE_1_OUT,
        DIM_TIME_2
    }

    public static Path getPath(PATH_ENUM pathEnum){
        try {
            switch (pathEnum) {
                case AGG_GEO_LAYER: {
                    return getPathWithJar(PATH_AGG_GEO_LAYER);
                }
                case STAGE_1_OUT: {
                    return getPathWithJar(PATH_STAGE_1_OUT);
                }
                case DIM_TIME_2: {
                    return getPathWithJar(PATH_DIM_TIME_2);
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
