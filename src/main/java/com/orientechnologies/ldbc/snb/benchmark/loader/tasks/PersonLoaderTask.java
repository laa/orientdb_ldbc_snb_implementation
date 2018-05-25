package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PersonLoaderTask extends AbstractLoaderTask<PersonDTO> {

  public PersonLoaderTask(ArrayBlockingQueue<PersonDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PersonDTO dto) {
    final OVertex personVertex = session.newVertex(DBLoader.PERSON_CLASS);

    personVertex.setProperty(DBLoader.PERSON_ID, dto.id);
    personVertex.setProperty(DBLoader.PERSON_FIRST_NAME, dto.firstName);
    personVertex.setProperty(DBLoader.PERSON_LAST_NAME, dto.lastName);
    personVertex.setProperty(DBLoader.PERSON_GENDER, dto.gender);
    personVertex.setProperty(DBLoader.PERSON_BIRTH_DATE, dto.birthDate);
    personVertex.setProperty(DBLoader.PERSON_BROWSER_USED, dto.browserUsed);
    personVertex.setProperty(DBLoader.PERSON_LOCATION_IP, dto.locationIP);
    personVertex.setProperty(DBLoader.PERSON_CREATION_DATE, dto.creationDate);

    personVertex.save();
  }
}
