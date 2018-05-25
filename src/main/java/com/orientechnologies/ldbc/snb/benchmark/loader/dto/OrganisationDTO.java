package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public abstract class OrganisationDTO extends AbstractDTO {
  public final String name;
  public final String url;

  OrganisationDTO(long id, boolean stop, String name, String url) {
    super(id, stop);
    this.name = name;
    this.url = url;
  }
}
