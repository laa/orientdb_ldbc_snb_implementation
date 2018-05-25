package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.ContinentDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class ContinentLoaderTask extends AbstractLoaderTask<ContinentDTO> {
  ContinentLoaderTask(ArrayBlockingQueue<ContinentDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, ContinentDTO dto) {
    final OVertex continentVertex = session.newVertex(DBLoader.CONTINENT_CLASS);

    continentVertex.setProperty(DBLoader.PLACE_ID, dto.id);
    continentVertex.setProperty(DBLoader.PLACE_NAME, dto.name);
    continentVertex.setProperty(DBLoader.PLACE_URL, dto.url);

    continentVertex.save();
  }
}
