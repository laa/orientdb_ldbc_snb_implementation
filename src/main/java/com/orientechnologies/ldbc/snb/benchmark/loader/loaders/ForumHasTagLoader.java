package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.ForumHasTagDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.ForumHasTagLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ForumHasTagLoader extends AbstractPartitionedLoader<ForumHasTagDTO> {
  public ForumHasTagLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<ForumHasTagDTO> createNewTask(ArrayBlockingQueue<ForumHasTagDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new ForumHasTagLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected ForumHasTagDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new ForumHasTagDTO(fromId, false, toId);
  }

  @Override
  protected ForumHasTagDTO createStopRecord() {
    return new ForumHasTagDTO(-1, true, -1);
  }
}
