package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.ContainerOfDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.ContainerOf;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ContainerOfLoaderTask extends AbstractLoaderTask<ContainerOfDTO> {
  public ContainerOfLoaderTask(ArrayBlockingQueue<ContainerOfDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, ContainerOfDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", ContainerOf.NAME,
            DBLoader.FORUM_CLASS, DBLoader.FORUM_ID, DBLoader.POST_CLASS, DBLoader.MESSAGE_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
