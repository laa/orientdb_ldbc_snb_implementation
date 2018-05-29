package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PlaceIsPartOfDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.PlaceIsPartOf;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PlaceIsPartOfLoaderTask extends AbstractLoaderTask<PlaceIsPartOfDTO> {
  public PlaceIsPartOfLoaderTask(ArrayBlockingQueue<PlaceIsPartOfDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, PlaceIsPartOfDTO dto) {
    final String query = String
        .format("create edge %s from (select from %s where %s = ?) to (select from %s where %s = ?)", PlaceIsPartOf.NAME,
            DBLoader.PLACE_CLASS, DBLoader.PLACE_ID, DBLoader.PLACE_CLASS, DBLoader.PLACE_ID);
    session.command(query, dto.id, dto.to).close();
  }
}
