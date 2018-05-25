package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public final class PlaceDTO extends AbstractDTO {
  public final String name;
  public final String url;
  public final String type;

  public PlaceDTO(long id, boolean stop, String name, String url, String type) {
    super(id, stop);
    this.name = name;
    this.url = url;
    this.type = type;
  }
}
