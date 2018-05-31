package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PlaceIsPartOfDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PlaceIsPartOfLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class PlaceIsPartOfLoader extends AbstractPartitionedLoader<PlaceIsPartOfDTO> {
  public PlaceIsPartOfLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<PlaceIsPartOfDTO> createNewTask(ArrayBlockingQueue<PlaceIsPartOfDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PlaceIsPartOfLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PlaceIsPartOfDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new PlaceIsPartOfDTO(fromId, false, toId);
  }

  @Override
  protected PlaceIsPartOfDTO createStopRecord() {
    return new PlaceIsPartOfDTO(-1, true, -1);
  }
}
