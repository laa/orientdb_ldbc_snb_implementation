package com.orientechnologies.ldbc.snb.benchmark.loader.dto;

import java.util.Date;

public final class PersonDTO extends AbstractDTO {
  public final String firstName;
  public final String lastName;
  public final String gender;
  public final Date   birthDate;
  public final String browserUsed;
  public final String locationIP;
  public final Date   creationDate;

  public PersonDTO(long id, String firstName, String lastName, String gender, Date birthDate, String browserUsed, String locationIP,
      Date creationDate, boolean stop) {
    super(id, stop);

    this.firstName = firstName;
    this.lastName = lastName;
    this.gender = gender;
    this.birthDate = birthDate;
    this.browserUsed = browserUsed;
    this.locationIP = locationIP;
    this.creationDate = creationDate;
  }
}
