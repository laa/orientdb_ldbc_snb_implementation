package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasInterestDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.HasInterestLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class HasInterestLoader extends AbstractPartitionedLoader<HasInterestDTO> {
  public HasInterestLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<HasInterestDTO> createNewTask(ArrayBlockingQueue<HasInterestDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new HasInterestLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected HasInterestDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new HasInterestDTO(fromId, false, toId);
  }

  @Override
  protected HasInterestDTO createStopRecord() {
    return new HasInterestDTO(-1, true, -1);
  }
}
