package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair;

import lombok.Getter;
import ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.MAddGrid;
import ru.atconsulting.bigdata.homejob.system.ClusterProperties;
import ru.atconsulting.bigdata.homejob.system.pojo.GeoLayer;

import java.util.*;

/**
 * Created by NSkovpin on 18.04.2017.
 */
@Getter
public class TopHolder {

    private boolean onlyTop1;

    private boolean notAsAll;

    /**
     * Long - time, triple.first = firstPartOf value, .second = secondPartOf value
     */
    private Pair<Long, Triple<String, String, String>> top1;
    /**
     * Long - time, triple.first = firstPartOf value, .second = secondPartOf value
     */
    private Pair<Long, Triple<String, String, String>> top2;

    public TopHolder() {
    }

    public TopHolder(boolean onlyTop1) {
        this.onlyTop1 = onlyTop1;
    }

    public TopHolder(boolean onlyTop1, boolean notAsAll) {
        this.onlyTop1 = onlyTop1;
        this.notAsAll = notAsAll;
    }


    public void addToTop(Long key, String[] valueRow, Long time, Long count) {
        if (key == null) {
            return;
        }
        if (top1 == null) {
            top1 = new Pair<>();
            top1.setKey(key);
            top1.setValue(createTopTriple(valueRow, time, count, notAsAll));
            return;
        }
        Long secondKey;
        Triple<String, String, String> secondValue;

        int comparisonTop1 = top1.getKey().compareTo(key);
        if (comparisonTop1 <= 0) { //same or greater input key
            secondKey = top1.getKey();
            secondValue = secondSortTriple(comparisonTop1, key, top1, valueRow, time, count, notAsAll);
        } else {
            secondKey = key;
            secondValue = createTopTriple(valueRow, time, count, notAsAll);
        }
        if (secondValue == null) {
            return;
        }

        if (!onlyTop1) {
            if (top2 == null) {
                top2 = new Pair<>();
                top2.setKey(secondKey);
                top2.setValue(secondValue);
            } else {
                int comparisonTop2 = top2.getKey().compareTo(secondKey);
                if (comparisonTop2 <= 0) {
                    secondSortTriple(comparisonTop2, secondKey, top2, secondValue);
                }
            }
        }
    }

    public String getTop1Value(int part) {
        switch (part) {
            case 1:
                return top1.getValue().getKey();
            case 2:
                return top1.getValue().getValue();
        }
        throw new RuntimeException("Don't know this part");
    }


    public String getTop2Value(int part) {
        if (onlyTop1) {
            throw new RuntimeException("Only top1 option for this holder");
        }
        switch (part) {
            case 1: {
                if (top2 == null) {
                    return getFirstPartEmpty(notAsAll);
                }
                return top2.getValue().getKey();
            }
            case 2: {
                if (top2 == null) {
                    return getSecondPartEmpty(notAsAll);
                }
                return top2.getValue().getValue();
            }
        }
        throw new RuntimeException("Don't know this part");
    }

    private Pair<String, String> createTopPair(String[] valueRow, Long time, Long count, boolean notAsAll) {
        Pair<String, String> pair = new Pair<>();
        addToKeyValue(pair, valueRow, time, count, notAsAll);
        return pair;
    }

    private Triple<String, String, String> createTopTriple(String[] valueRow, Long time, Long count, boolean notAsAll) {
        Triple<String, String, String> triple = new Triple<>();
        addToKeyValue(triple, valueRow, time, count, notAsAll);
        triple.setAddition(getOrEmpty(valueRow[MAddGrid.OutputValue.CELL_LIST.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND));
        return triple;
    }


    private void addToKeyValue(KeyValueInter<String, String> keyValueInter, String[] valueRow, Long time, Long count, boolean notAsAll) {
        if (notAsAll) {
            keyValueInter.setKey(getOrEmpty(valueRow[MAddGrid.OutputValue.LAC.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.CELL_ID.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.LATITUDE.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.LONGITUDE.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.FULL_ADDRESS.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(count, ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.CELL_LIST.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND));
            keyValueInter.setValue(getOrEmpty(time, ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.LOCALITY_NAME.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.BRANCH_ID.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.CITY_ID.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.GRID_LIST.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND));
        } else {
            keyValueInter.setKey(getOrEmpty(valueRow[MAddGrid.OutputValue.LAC.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.CELL_ID.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.LATITUDE.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.LONGITUDE.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.FULL_ADDRESS.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.CELL_LIST.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND));
            keyValueInter.setValue(getOrEmpty(count, ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(time, ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.LOCALITY_NAME.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.BRANCH_ID.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.CITY_ID.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND)
                    + GeoLayer.Constant.FIELD_DELIMITER +
                    getOrEmpty(valueRow[MAddGrid.OutputValue.GRID_LIST.ordinal()], ClusterProperties.EMPTY_LOCATION_NOT_FOUND));
        }
    }

    private String getFirstPartEmpty(boolean notAsAll) {
        if (notAsAll) {
            return ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND;
        } else {
            return ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND;
        }
    }

    private String getSecondPartEmpty(boolean notAsAll) {
        if (notAsAll) {
            return ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND;
        } else {
            return ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND + GeoLayer.Constant.FIELD_DELIMITER +
                    ClusterProperties.EMPTY_LOCATION_NOT_FOUND;
        }

    }

    private String getOrEmpty(Object value, String emptyValue) {
        if (value == null) {
            return emptyValue;
        }
        if (value instanceof String) {
            String v = (String) value;
            if (v.isEmpty()) {
                return emptyValue;
            }
        }
        return "" + value;
    }

    private Pair<String, String> secondSortPair(int firstComparison, Long key, Pair<Long, Pair<String, String>> top,
                                                String[] valueRow, Long time, Long count, boolean notAsAll) {
        Pair<String, String> inputPair = createTopPair(valueRow, time, count, notAsAll);
        return secondSortPair(firstComparison, key, top, inputPair);
    }

    private Pair<String, String> secondSortPair(int firstComparison, Long key, Pair<Long, Pair<String, String>> top,
                                                Pair<String, String> inputPair) {
        if (firstComparison == 0) {
            int comparisonLacCell = top.getValue().getKey().compareToIgnoreCase(inputPair.getKey());
            if (comparisonLacCell < 0) {
                Pair<String, String> previous = top.getValue();
                top.setValue(inputPair);
                return previous;
            }
        } else {
            Pair<String, String> previous = top.getValue();
            top.setKey(key);
            top.setValue(inputPair);
            return previous;
        }
        return inputPair;
    }


    private Triple<String, String, String> secondSortTriple(int firstComparison, Long key, Pair<Long, Triple<String, String, String>> top,
                                                            String[] valueRow, Long time, Long count, boolean notAsAll) {
        Triple<String, String, String> inputPair = createTopTriple(valueRow, time, count, notAsAll);
        return secondSortTriple(firstComparison, key, top, inputPair);
    }

    private Triple<String, String, String> secondSortTriple(int firstComparison, Long key, Pair<Long, Triple<String, String, String>> top,
                                                            Triple<String, String, String> inputPair) {
        if (firstComparison == 0) {

            boolean isDistinct = isDistinctCellLists(top.getValue(), inputPair);

            if (inputHasLacCell(top.getValue().getKey(), inputPair.getKey())) {
                Triple<String, String, String> previous = top.getValue();
                top.setValue(inputPair);
                return previous;
            }
            if (inputHasntLacCell(top.getValue().getKey(), inputPair.getKey())) {
                return inputPair;
            }

            int comparisonLacCell = getLacCell(top.getValue().getKey()).compareToIgnoreCase(getLacCell(inputPair.getKey()));
            if (comparisonLacCell < 0) {
                Triple<String, String, String> previous = top.getValue();
                top.setValue(inputPair);

                if (!isDistinct) {
                    return null;
                }
                return previous;
            }
        } else {
            Triple<String, String, String> previous = top.getValue();
            top.setKey(key);
            top.setValue(inputPair);
            return previous;
        }
        return inputPair;
    }

    private boolean isDistinctCellLists(Triple<String, String, String> triple1, Triple<String, String, String> triple2) {
        Set<String> firstSet = new HashSet<>(Arrays.asList(triple1.getAddition().split(GeoLayer.Constant.CELL_LIST_DELIMITER, -1)));
        Set<String> secondSet = new HashSet<>(Arrays.asList(triple2.getAddition().split(GeoLayer.Constant.CELL_LIST_DELIMITER, -1)));
        firstSet.retainAll(secondSet);
        return firstSet.size() == 0;
    }

    private boolean inputHasLacCell(String key1, String key2) {
        return key1.startsWith(ClusterProperties.EMPTY_LOCATION_NOT_FOUND) && !key2.startsWith(ClusterProperties.EMPTY_LOCATION_NOT_FOUND);
    }

    private boolean inputHasntLacCell(String key1, String key2) {
        return !key1.startsWith(ClusterProperties.EMPTY_LOCATION_NOT_FOUND) && key2.startsWith(ClusterProperties.EMPTY_LOCATION_NOT_FOUND);
    }

    private String getLacCell(String key) {
        String keyArray[] = key.split(GeoLayer.Constant.FIELD_DELIMITER, -1);
        return keyArray[HOME_INDEXIES.LAC.ordinal()] + keyArray[HOME_INDEXIES.CELL.ordinal()];
    }

    public enum HOME_INDEXIES {
        LAC, CELL, LAT, LON, ADDRESS, PERIOD, CELL_LIST
    }

}