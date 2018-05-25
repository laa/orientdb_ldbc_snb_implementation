package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public final class TagClassDTO extends AbstractDTO {
  public final String name;
  public final String url;

  public TagClassDTO(long id, boolean stop, String name, String url) {
    super(id, stop);
    this.name = name;
    this.url = url;
  }
}
