package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.StudyAtDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.StudyAt;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class StudyAtLoaderTask extends AbstractLoaderTask<StudyAtDTO> {
  public StudyAtLoaderTask(ArrayBlockingQueue<StudyAtDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, StudyAtDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?) set %s = ?", StudyAt.NAME,
            DBLoader.PERSON_CLASS, DBLoader.PERSON_ID, DBLoader.ORGANISATION_CLASS, DBLoader.ORGANISATION_ID, StudyAt.CLASS_YEAR);
    session.command(query, dto.id, dto.to, dto.classYear).close();
  }
}
