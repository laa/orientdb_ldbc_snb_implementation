package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.HasMemberDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.HasMemberLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVRecord;

import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class HasMemberLoader extends AbstractPartitionedLoader<HasMemberDTO> {
  public HasMemberLoader(Path dataDir, String filePattern, int loadNumber, int totalAmountOfLoads) {
    super(dataDir, filePattern, loadNumber, totalAmountOfLoads);
  }

  @Override
  protected AbstractLoaderTask<HasMemberDTO> createNewTask(ArrayBlockingQueue<HasMemberDTO> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter) {
    return new HasMemberLoaderTask(dataQueue, pool, operationsCounter);
  }

  @Override
  protected HasMemberDTO parseCSVRecord(CSVRecord csvRecord) {
    final long fromId = Long.parseLong(csvRecord.get(0));
    final long toId = Long.parseLong(csvRecord.get(1));
    final Date joinDate = DateUtils.convertDateTime(csvRecord.get(2));

    return new HasMemberDTO(fromId, false, toId, joinDate);
  }

  @Override
  protected HasMemberDTO createStopRecord() {
    return new HasMemberDTO(-1, true, -1, null);
  }
}
