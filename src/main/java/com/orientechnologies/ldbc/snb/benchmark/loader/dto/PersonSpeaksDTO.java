package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public final class PersonSpeaksDTO extends AbstractDTO {
  public final String speaks;

  public PersonSpeaksDTO(long id, String speaks, boolean stop) {
    super(id, stop);

    this.speaks = speaks;
  }
}
