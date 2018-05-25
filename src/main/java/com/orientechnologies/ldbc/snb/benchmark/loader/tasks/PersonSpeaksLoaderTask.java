package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonSpeaksDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PersonSpeaksLoaderTask extends AbstractLoaderTask<PersonSpeaksDTO> {
  public PersonSpeaksLoaderTask(ArrayBlockingQueue<PersonSpeaksDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PersonSpeaksDTO dto) {
    session.command(
        "update " + DBLoader.PERSON_CLASS + " set " + DBLoader.PERSON_SPEAKS + " = " + DBLoader.PERSON_SPEAKS + " || ? where "
            + DBLoader.PERSON_ID + "  = ?", dto.speaks, dto.id).close();
  }
}
