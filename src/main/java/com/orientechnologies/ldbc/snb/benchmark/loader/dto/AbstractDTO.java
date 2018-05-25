package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public abstract class AbstractDTO {
  public final long id;
  public final boolean stop;

  public AbstractDTO(long id, boolean stop) {
    this.id = id;
    this.stop = stop;
  }
}
