package com.orientechnologies.ldbc.snb.benchmark.loader.loaders;

import com.orientechnologies.ldbc.snb.benchmark.loader.dto.AbstractDTO;
import com.orientechnologies.ldbc.snb.benchmark.loader.tasks.AbstractLoaderTask;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractLoader<D extends AbstractDTO> {
  final Path   dataDir;
  final String filePattern;

  AbstractLoader(Path dataDir, String filePattern) {
    this.dataDir = dataDir;
    this.filePattern = filePattern;
  }

  public void loadData(ODatabasePool pool, ExecutorService executor) throws IOException, ExecutionException, InterruptedException {
    final ArrayBlockingQueue<D> dataQueue = new ArrayBlockingQueue<>(1024);
    final List<Future<Void>> futures = new ArrayList<>();
    final int numThreads = 8;

    final String loaderName = this.getClass().getSimpleName();
    System.out.printf("%tc : Start loading of data from '%s' files from directory %s using %s.\n", System.currentTimeMillis(),
        filePattern, dataDir, loaderName);

    final AtomicLong operationsCounter = new AtomicLong();
    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(createNewTask(dataQueue, pool, operationsCounter)));
    }

    final Timer statusTimer = startStatusTimer(operationsCounter);
    final long start = System.nanoTime();

    Files.list(dataDir).filter((path) -> path.getFileName().toString().matches(filePattern)).
        forEach(path -> {
          try {
            try (InputStream is = Files.newInputStream(path)) {
              try (InputStreamReader isr = new InputStreamReader(is)) {
                try (BufferedReader br = new BufferedReader(isr)) {
                  try (CSVParser csvParser = new CSVParser(br, CSVFormat.DEFAULT.withSkipHeaderRecord().withFirstRecordAsHeader().withDelimiter('|'))) {
                    for (CSVRecord csvRecord : csvParser) {
                      dataQueue.put(parseCSVRecord(csvRecord));
                    }
                  }
                }
              }
            }

            final D stopRecord = createStopRecord();
            for (int i = 0; i < numThreads; i++) {
              dataQueue.put(stopRecord);
            }

          } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
          }
        });

    for (Future<Void> future : futures) {
      future.get();
    }

    final long end = System.nanoTime();
    stopStatusTimer(statusTimer);

    final long timePassed = end - start;
    final long operations = operationsCounter.get();

    final int[] passedTime = DateUtils.convertIntervalInHoursMinSec(timePassed);
    final long timePerOperation = timePassed / operations;
    final long throughput = DateUtils.NANOS_IN_SECONDS / timePerOperation;
    final long operationTimeMks = timePerOperation / 1_000;

    System.out.printf("%tc : %s : Loading is completed in %d h. %d m. %d s. Avg operation time %d us. Throughput %d op/s. "
            + "Total operations %d.\n",
        System.currentTimeMillis(), loaderName, passedTime[0], passedTime[1], passedTime[2], operationTimeMks,
        throughput, operations);
  }

  protected abstract AbstractLoaderTask<D> createNewTask(ArrayBlockingQueue<D> dataQueue, ODatabasePool pool,
      AtomicLong operationsCounter);

  protected abstract D parseCSVRecord(CSVRecord csvRecord);

  protected abstract D createStopRecord();

  Timer startStatusTimer(AtomicLong operationsCounter) {
    final Timer statusTimer = new Timer();

    statusTimer.schedule(new TimerTask() {
      long ts = -1;
      long operations = -1;

      @Override
      public void run() {
        if (ts == -1) {
          ts = System.nanoTime();
          operations = operationsCounter.get();
        } else {
          final long currentTs = System.nanoTime();
          final long currentOperations = operationsCounter.get();

          final long operationsPassed = currentOperations - operations;
          final long timePassed = currentTs - ts;

          ts = currentTs;
          operations = currentOperations;

          if (operationsPassed == 0) {
            System.out.printf("%tc : %d operations, avg. operation time 0 us, throughput N/A\n", System.currentTimeMillis(),
                currentOperations);
          } else {
            final long operationTime = timePassed / operationsPassed;
            final long throughput = DateUtils.NANOS_IN_SECONDS / operationTime;
            final long operationTimeInMks = operationTime / 1_000;
            System.out.printf("%tc : %d operations, avg. operation time %d us, throughput %d op/s\n", System.currentTimeMillis(),
                currentOperations, operationTimeInMks, throughput);
          }
        }
      }
    }, 10, 10 * 1000); // report every 10 seconds.

    return statusTimer;
  }

  void stopStatusTimer(Timer timer) {
    timer.cancel();
  }
}
