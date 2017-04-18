package ru.atconsulting.bigdata.homejob.staging.stage_3_group_ctn.pair;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 18.04.2017.
 */
@Getter
@Setter
class Pair<K, V> {
    K key;
    V value;
}