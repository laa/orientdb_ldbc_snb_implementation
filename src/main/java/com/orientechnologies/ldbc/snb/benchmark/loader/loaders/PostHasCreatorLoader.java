package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PostHasCreatorDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PostHasCreatorLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PostHasCreatorLoader extends AbstractPartitionedLoader<PostHasCreatorDTO> {
  public PostHasCreatorLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PostHasCreatorDTO> createNewTask(ArrayBlockingQueue<PostHasCreatorDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new PostHasCreatorLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PostHasCreatorDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new PostHasCreatorDTO(fromId, false, toId);
  }

  @Override
  protected PostHasCreatorDTO createStopRecord() {
    return new PostHasCreatorDTO(-1, true, -1);
  }
}
