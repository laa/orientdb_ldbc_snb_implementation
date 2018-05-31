package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentHasCreatorDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.CommentHasCreatorLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentHasCreatorLoader extends AbstractPartitionedLoader<CommentHasCreatorDTO> {
  public CommentHasCreatorLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<CommentHasCreatorDTO> createNewTask(ArrayBlockingQueue<CommentHasCreatorDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new CommentHasCreatorLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected CommentHasCreatorDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new CommentHasCreatorDTO(fromId, false, toId);
  }

  @Override
  protected CommentHasCreatorDTO createStopRecord() {
    return new CommentHasCreatorDTO(-1, true, -1);
  }
}
