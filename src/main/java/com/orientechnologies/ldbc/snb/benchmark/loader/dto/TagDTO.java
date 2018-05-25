package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public final class TagDTO extends AbstractDTO {
  public final String name;
  public final String url;

  public TagDTO(long id, boolean stop, String name, String url) {
    super(id, stop);
    this.name = name;
    this.url = url;
  }
}
