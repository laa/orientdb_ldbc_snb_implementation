package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.ForumDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class ForumLoaderTask extends AbstractLoaderTask<ForumDTO> {
  ForumLoaderTask(ArrayBlockingQueue<ForumDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, ForumDTO dto) {
    final OVertex forumVertex = session.newVertex(DBLoader.FORUM_CLASS);

    forumVertex.setProperty(DBLoader.FORUM_ID, dto.id);
    forumVertex.setProperty(DBLoader.FORUM_TITLE, dto.title);
    forumVertex.setProperty(DBLoader.FORUM_CREATION_DATE, dto.creationDate);

    forumVertex.save();
  }
}
