package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentReplyOfPostDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.CommentReplyOfPostLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentReplyOfPostLoader extends AbstractPartitionedLoader<CommentReplyOfPostDTO> {
  public CommentReplyOfPostLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<CommentReplyOfPostDTO> createNewTask(ArrayBlockingQueue<CommentReplyOfPostDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new CommentReplyOfPostLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected CommentReplyOfPostDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new CommentReplyOfPostDTO(fromId, false, toId);
  }

  @Override
  protected CommentReplyOfPostDTO createStopRecord() {
    return new CommentReplyOfPostDTO(-1, true, -1);
  }
}
