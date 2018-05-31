package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentHasTagDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.CommentHasTagLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class CommentHasTagLoader extends AbstractPartitionedLoader<CommentHasTagDTO> {
  public CommentHasTagLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<CommentHasTagDTO> createNewTask(ArrayBlockingQueue<CommentHasTagDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new CommentHasTagLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected CommentHasTagDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new CommentHasTagDTO(fromId, false, toId);
  }

  @Override
  protected CommentHasTagDTO createStopRecord() {
    return new CommentHasTagDTO(-1, true, -1);
  }
}
