package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair;

import java.util.Comparator;

/**
 * Created by NSkovpin on 18.04.2017.
 */
public class PairComparator implements Comparator<Pair<Long, String>> {

    @Override
    public int compare(Pair<Long, String> o1, Pair<Long, String> o2) {
        if (o1 == null) {
            return -1;
        } else if (o2 == null) {
            return 1;
        }
        return o1.getKey().compareTo(o2.getKey());
    }

}
