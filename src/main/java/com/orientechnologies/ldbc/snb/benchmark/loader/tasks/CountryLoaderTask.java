package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.ldbc.snb.benchmark.loader.DBLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CountryDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OVertex;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class CountryLoaderTask extends AbstractLoaderTask<CountryDTO> {
  CountryLoaderTask(ArrayBlockingQueue<CountryDTO> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    super(dataQueue, pool, operationsCounter);
  }

  @Override
  protected void execute(ODatabaseSession session, CountryDTO dto) {
    final OVertex countryVertex = session.newVertex(DBLoader.COUNTRY_CLASS);

    countryVertex.setProperty(DBLoader.PLACE_ID, dto.id);
    countryVertex.setProperty(DBLoader.PLACE_NAME, dto.name);
    countryVertex.setProperty(DBLoader.PLACE_URL, dto.url);

    countryVertex.save();
  }
}
