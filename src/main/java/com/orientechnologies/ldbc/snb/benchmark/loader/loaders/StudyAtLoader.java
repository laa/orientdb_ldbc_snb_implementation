package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.StudyAtDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.StudyAtLoaderTask;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class StudyAtLoader extends AbstractPartitionedLoader<StudyAtDTO> {
  public StudyAtLoader(Path dataDir, String filePattern) {
    super(dataDir, filePattern);
  }

  @Override
  protected AbstractLoaderTask<StudyAtDTO> createNewTask(ArrayBlockingQueue<StudyAtDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new StudyAtLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected StudyAtDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));
    final int classYear = Integer.parseInt(csvRecord.get(2));

    return new StudyAtDTO(fromId, false, toId, classYear);
  }

  @Override
  protected StudyAtDTO createStopRecord() {
    return new StudyAtDTO(-1, true, -1, -1);
  }
}
