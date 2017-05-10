package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by NSkovpin on 18.04.2017.
 */
@Getter
@Setter
public class Pair<K, V> implements Serializable, KeyValueInter<K,V>{
    private K key;
    private V value;

    public static<K, V> Pair<K, V> of(K k, V v){
        Pair<K, V> pair = new Pair<>();
        pair.setKey(k);
        pair.setValue(v);
        return pair;
    }
}