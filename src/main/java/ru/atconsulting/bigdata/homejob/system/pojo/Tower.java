package ru.atconsulting.bigdata.homejob.system.pojo;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@Builder
@EqualsAndHashCode
public class Tower{
    private String lac;
    private String cellId;
}
