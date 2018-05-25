package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.CommentReplyOfCommentDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.CommentReplyOfCommentLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CommentReplyOfCommentLoader extends AbstractPartitionedLoader<CommentReplyOfCommentDTO> {
  public CommentReplyOfCommentLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<CommentReplyOfCommentDTO> createNewTask(ArrayBlockingQueue<CommentReplyOfCommentDTO> dataQueue,
      ODatabasePool pool, AtomicLong operationsCounter) {
    return new CommentReplyOfCommentLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected CommentReplyOfCommentDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));

    return new CommentReplyOfCommentDTO(fromId, false, toId);
  }

  @Override
  protected CommentReplyOfCommentDTO createStopRecord() {
    return new CommentReplyOfCommentDTO(-1, true, -1);
  }
}
