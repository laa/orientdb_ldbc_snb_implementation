package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

public final class PersonEMailDTO extends AbstractDTO {
  public final String email;

  public PersonEMailDTO(long id, String email, boolean stop) {
    super(id, stop);

    this.email = email;
  }
}
