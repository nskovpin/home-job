package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair;

import lombok.Getter;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.MAddGrid;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;

/**
 * Created by NSkovpin on 18.04.2017.
 */
@Getter
public class TopHolder {

    private boolean onlyTop1;
    private Pair<Long, Pair<String, String>> top1;
    private Pair<Long, Pair<String, String>> top2;

    public TopHolder() {
    }

    public TopHolder(boolean onlyTop1) {
        this.onlyTop1 = onlyTop1;
    }

    public void addToTop(Long key, String[] valueRow, Long time, Long count) {
        if (key == null) {
            return;
        }
        if (top1 == null) {
            top1 = new Pair<>();
            top1.setKey(key);
            top1.setValue(createTopString(valueRow, time, count));
            return;
        }
        if (top1.getKey().compareTo(key) < 0) {
            top1.setKey(key);
            top1.setValue(createTopString(valueRow, time, count));
        }
        if (!onlyTop1) {
            if (top2 == null) {
                top2 = new Pair<>();
                top2.setKey(key);
                top2.setValue(createTopString(valueRow, time, count));
            } else {
                if (top2.getKey().compareTo(key) < 0) {
                    top2.setKey(key);
                    top2.setValue(createTopString(valueRow, time, count));
                }
            }
        }
    }

    public String getTop1Value(int part) {
        switch (part){
            case 1 : return top1.getValue().getKey();
            case 2 : return top2.getValue().getValue();
        }
        throw new RuntimeException("Don't know this part");
    }


    public String getTop2Value(int part) {
        if (onlyTop1) {
            throw new RuntimeException("Only top1 option for this holder");
        }
        switch (part){
            case 1 : {
                if(top2 == null){
                    return getFirstPartEmpty();
                }
                return top2.getValue().getKey();
            }
            case 2 : {
                if(top2 == null){
                    return getSecondPartEmpty();
                }
                return top2.getValue().getValue();
            }
        }
        throw new RuntimeException("Don't know this part");
    }

    private Pair<String, String> createTopString(String[] valueRow, Long time, Long count) {
        Pair<String, String> pair = new Pair<>();
        pair.setKey(valueRow[MAddGrid.OutputValue.LAC.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.CELL_ID.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.LATITUDE.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.LONGITUDE.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.FULL_ADDRESS.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.CELL_LIST.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.CELL_ID.ordinal()]);
        pair.setValue(count + GeoLayer.Constant.FIELD_DELIMITER +
                time + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.LOCALITY_NAME.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.BRANCH_ID.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.CITY_ID.ordinal()] + GeoLayer.Constant.FIELD_DELIMITER +
                valueRow[MAddGrid.OutputValue.GRID_LIST.ordinal()]);
        return pair;
    }

    private String getFirstPartEmpty() {
        return "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "";

    }

    private String getSecondPartEmpty() {
        return "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "" + GeoLayer.Constant.FIELD_DELIMITER +
                "";
    }

}