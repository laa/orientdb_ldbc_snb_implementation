package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PlaceDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PlaceLoaderTask extends AbstractLoaderTask<PlaceDTO> {
  public PlaceLoaderTask(ArrayBlockingQueue<PlaceDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PlaceDTO dto) {
    final OVertex placeVertex;
    switch (dto.type) {
    case "city":
      placeVertex = session.newVertex(DBLoader.CITY_CLASS);
      break;
    case "country":
      placeVertex = session.newVertex(DBLoader.COUNTRY_CLASS);
      break;
    case "continent":
      placeVertex = session.newVertex(DBLoader.CONTINENT_CLASS);
      break;
    default:
      throw new IllegalArgumentException("Invalid entry type " + dto.type);
    }

    placeVertex.setProperty(DBLoader.PLACE_ID, dto.id);

    placeVertex.setProperty(DBLoader.PLACE_NAME, dto.name);
    placeVertex.setProperty(DBLoader.PLACE_URL, dto.url);

    placeVertex.save();
  }
}
