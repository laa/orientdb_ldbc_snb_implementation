package com.orientechnologies.ldbc.snb.benchmark.loader.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;

public interface HasMember {
  String NAME = "hasMember";

  String JOIN_DATE      = "joinDate";
  OType  JOIN_DATE_TYPE = OType.DATETIME;
}
