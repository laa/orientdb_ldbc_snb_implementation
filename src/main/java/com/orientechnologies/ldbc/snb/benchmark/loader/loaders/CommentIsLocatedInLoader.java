package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentIsLocatedInDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.CommentIsLocatedInLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentIsLocatedInLoader extends AbstractPartitionedLoader<CommentIsLocatedInDTO> {
  public CommentIsLocatedInLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<CommentIsLocatedInDTO> createNewTask(ArrayBlockingQueue<CommentIsLocatedInDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new CommentIsLocatedInLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected CommentIsLocatedInDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new CommentIsLocatedInDTO(fromId, false, toId);
  }

  @Override
  protected CommentIsLocatedInDTO createStopRecord() {
    return new CommentIsLocatedInDTO(-1, true, -1);
  }
}
