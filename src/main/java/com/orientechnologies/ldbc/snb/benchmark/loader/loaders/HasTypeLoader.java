package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasTypeDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.HasTypeLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class HasTypeLoader extends AbstractPartitionedLoader<HasTypeDTO> {
  public HasTypeLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<HasTypeDTO> createNewTask(ArrayBlockingQueue<HasTypeDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new HasTypeLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected HasTypeDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new HasTypeDTO(fromId, false, toId);
  }

  @Override
  protected HasTypeDTO createStopRecord() {
    return new HasTypeDTO(-1, true, -1);
  }
}
