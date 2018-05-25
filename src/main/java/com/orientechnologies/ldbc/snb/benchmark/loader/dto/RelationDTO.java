package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public abstract class RelationDTO extends AbstractDTO {
  public final long to;

  RelationDTO(long id, boolean stop, long to) {
    super(id, stop);
    this.to = to;
  }
}
