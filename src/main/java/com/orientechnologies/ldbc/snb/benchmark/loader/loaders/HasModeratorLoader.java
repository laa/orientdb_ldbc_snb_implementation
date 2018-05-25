package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasModeratorDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.HasModeratorLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class HasModeratorLoader extends AbstractPartitionedLoader<HasModeratorDTO> {
  public HasModeratorLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<HasModeratorDTO> createNewTask(ArrayBlockingQueue<HasModeratorDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new HasModeratorLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected HasModeratorDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new HasModeratorDTO(fromId, false, toId);
  }

  @Override
  protected HasModeratorDTO createStopRecord() {
    return new HasModeratorDTO(-1, true, -1);
  }
}
