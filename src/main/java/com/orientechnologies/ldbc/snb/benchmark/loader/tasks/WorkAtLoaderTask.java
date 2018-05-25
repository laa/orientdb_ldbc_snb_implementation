package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.WorkAtDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.StudyAt;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.WorkAt;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class WorkAtLoaderTask extends AbstractLoaderTask<WorkAtDTO> {
  public WorkAtLoaderTask(ArrayBlockingQueue<WorkAtDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, WorkAtDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?) set %s = ?", WorkAt.NAME,
            DBLoader.PERSON_CLASS, DBLoader.PERSON_ID, DBLoader.ORGANISATION_CLASS, DBLoader.ORGANISATION_ID, WorkAt.WORK_FROM);
    session.command(query, dto.id, dto.to, dto.workFrom).close();
  }
}
