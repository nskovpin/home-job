package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair;

/**
 * Created by NSkovpin on 25.04.2017.
 */
public interface KeyValueInter<K, V> {
    void setKey(K key);

    void setValue(V value);
}
