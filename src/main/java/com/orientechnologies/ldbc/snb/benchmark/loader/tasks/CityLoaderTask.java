package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CityDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class CityLoaderTask extends AbstractLoaderTask<CityDTO> {
  CityLoaderTask(ArrayBlockingQueue<CityDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CityDTO dto) {
    final OVertex cityVertex = session.newVertex(DBLoader.CITY_CLASS);

    cityVertex.setProperty(DBLoader.PLACE_ID, dto.id);
    cityVertex.setProperty(DBLoader.PLACE_NAME, dto.name);
    cityVertex.setProperty(DBLoader.PLACE_URL, dto.url);

    cityVertex.save();
  }
}
