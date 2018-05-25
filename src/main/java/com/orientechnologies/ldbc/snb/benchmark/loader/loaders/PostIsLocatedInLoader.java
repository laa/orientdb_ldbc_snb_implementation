package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.PostIsLocatedInDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.PostIsLocatedInLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PostIsLocatedInLoader extends AbstractPartitionedLoader<PostIsLocatedInDTO> {
  public PostIsLocatedInLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<PostIsLocatedInDTO> createNewTask(ArrayBlockingQueue<PostIsLocatedInDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new PostIsLocatedInLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected PostIsLocatedInDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new PostIsLocatedInDTO(fromId, false, toId);
  }

  @Override
  protected PostIsLocatedInDTO createStopRecord() {
    return new PostIsLocatedInDTO(-1, true, -1);
  }
}
