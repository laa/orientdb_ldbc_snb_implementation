package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PlaceDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PlaceLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PlaceLoader extends AbstractLoader<PlaceDTO> {
  public PlaceLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PlaceDTO> createNewTask(ArrayBlockingQueue<PlaceDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PlaceLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PlaceDTO parseCSVRecord(CSVRecord csvRecord) {
    final long id = Long.parseLong(csvRecord.get(0));
    final String name = csvRecord.get(1);
    final String url = csvRecord.get(2);
    final String type = csvRecord.get(3);

    return new PlaceDTO(id, false, name, url, type);
  }

  @Override
  protected PlaceDTO createStopRecord() {
    return new PlaceDTO(-1, true, null, null, null);
  }
}
