package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class CommentLoaderTask extends AbstractLoaderTask<CommentDTO> {
  public CommentLoaderTask(ArrayBlockingQueue<CommentDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CommentDTO dto) {
    OVertex commentVertex = session.newVertex(DBLoader.COMMENT_CLASS);

    commentVertex.setProperty(DBLoader.MESSAGE_ID, dto.id);
    commentVertex.setProperty(DBLoader.MESSAGE_BROWSER_USED, dto.browserUsed);
    commentVertex.setProperty(DBLoader.MESSAGE_CONTENT, dto.content);
    commentVertex.setProperty(DBLoader.MESSAGE_CREATION_DATE, dto.creationDate);
    commentVertex.setProperty(DBLoader.MESSAGE_LENGTH, dto.length);
    commentVertex.setProperty(DBLoader.MESSAGE_LOCATION_IP, dto.locationIP);

    commentVertex.save();
  }
}
