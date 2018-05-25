package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public class WorkAtDTO extends RelationDTO {
  public final int workFrom;

  public WorkAtDTO(long id, boolean stop, long to, int workFrom) {
    super(id, stop, to);
    this.workFrom = workFrom;
  }
}
