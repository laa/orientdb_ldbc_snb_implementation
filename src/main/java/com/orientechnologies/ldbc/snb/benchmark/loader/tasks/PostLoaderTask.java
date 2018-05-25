package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PostDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PostLoaderTask extends AbstractLoaderTask<PostDTO> {
  public PostLoaderTask(ArrayBlockingQueue<PostDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PostDTO dto) {
    final OVertex postVertex = session.newVertex(DBLoader.POST_CLASS);

    postVertex.setProperty(DBLoader.MESSAGE_ID, dto.id);
    postVertex.setProperty(DBLoader.MESSAGE_LOCATION_IP, dto.locationIP);
    postVertex.setProperty(DBLoader.MESSAGE_LENGTH, dto.length);
    postVertex.setProperty(DBLoader.MESSAGE_CREATION_DATE, dto.creationDate);
    postVertex.setProperty(DBLoader.MESSAGE_CONTENT, dto.content);
    postVertex.setProperty(DBLoader.MESSAGE_BROWSER_USED, dto.browserUsed);

    postVertex.setProperty(DBLoader.POST_LANGUAGE, dto.language);
    postVertex.setProperty(DBLoader.POST_IMAGE_FILE, dto.imageFile);

    postVertex.save();
  }
}
