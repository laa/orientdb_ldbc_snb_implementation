package com.orientechnologies.ldbc.snb.benchmark.loader.tasks;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.ldbc.snb.benchmark.loader.dto.AbstractDTO;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractLoaderTask<D extends AbstractDTO> implements Callable<Void> {
  private final ArrayBlockingQueue<D> dataQueue;
  private final ODatabasePool         pool;
  private final AtomicLong            operationsCounter;

  AbstractLoaderTask(ArrayBlockingQueue<D> dataQueue, ODatabasePool pool, AtomicLong operationsCounter) {
    this.dataQueue = dataQueue;
    this.pool = pool;
    this.operationsCounter = operationsCounter;
  }

  @Override
  public Void call() throws Exception {
    try {
      while (true) {
        final D dto = dataQueue.take();
        if (dto.stop) {
          return null;
        }

        try (ODatabaseSession session = pool.acquire()) {
          while (true) {
            try {
              execute(session, dto);
              operationsCounter.incrementAndGet();
              break;
            } catch (ONeedRetryException e) {
              //retry
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  protected abstract void execute(ODatabaseSession session, D dto);
}
