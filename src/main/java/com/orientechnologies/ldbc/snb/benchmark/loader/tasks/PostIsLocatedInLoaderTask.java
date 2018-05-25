package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PostIsLocatedInDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageIsLocatedIn;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PostIsLocatedInLoaderTask extends AbstractLoaderTask<PostIsLocatedInDTO> {
  public PostIsLocatedInLoaderTask(ArrayBlockingQueue<PostIsLocatedInDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PostIsLocatedInDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", MessageIsLocatedIn.NAME,
            DBLoader.POST_CLASS, DBLoader.MESSAGE_ID, DBLoader.PLACE_CLASS, DBLoader.PLACE_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
