package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by NSkovpin on 20.04.2017.
 */
@Getter
@Setter
class Triple<K, V , U> implements Serializable, KeyValueInter<K, V>{
    private K key;
    private V value;
    private U addition;
}
