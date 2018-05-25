package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PersonEMailDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PersonEMailLoaderTask extends AbstractLoaderTask<PersonEMailDTO> {
  public PersonEMailLoaderTask(ArrayBlockingQueue<PersonEMailDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PersonEMailDTO dto) {
    session.command(
        "update " + DBLoader.PERSON_CLASS + " set " + DBLoader.PERSON_EMAIL + " = " + DBLoader.PERSON_EMAIL + " || ? where "
            + DBLoader.PERSON_ID + "  = ?", dto.email, dto.id).close();
  }
}
