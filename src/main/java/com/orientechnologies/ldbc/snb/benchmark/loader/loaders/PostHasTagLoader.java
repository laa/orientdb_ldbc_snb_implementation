package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PostHasTagDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PostHasTagLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PostHasTagLoader extends AbstractPartitionedLoader<PostHasTagDTO> {
  public PostHasTagLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PostHasTagDTO> createNewTask(ArrayBlockingQueue<PostHasTagDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PostHasTagLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PostHasTagDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new PostHasTagDTO(fromId, false, toId);
  }

  @Override
  protected PostHasTagDTO createStopRecord() {
    return new PostHasTagDTO(-1, true, -1);
  }
}
