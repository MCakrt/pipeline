package com.snapscore.pipeline.concurrency;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class LockingWrapperTest {

    Map<Integer, Integer> map = new HashMap<>();

    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = reentrantReadWriteLock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
    private final Random random = new Random(1000);

    @Before
    public void setUp() throws Exception {
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
    }

    @Ignore
    @Test
    public void testLockedReadingVsNotLockedReadingPerformance() {
        timeUnlockedAccess();
        timeLockedAccess();
    }

    private void timeUnlockedAccess() {
        long start = System.currentTimeMillis();
        Runnable runnable = () -> {
            for (int j = 0; j < 100000; j++) {
                Supplier<Optional<Integer>> supplier = () -> Optional.ofNullable(map.get(random.nextInt()));
                supplier.get();
            }
        };
        executeInParallel(runnable);
        long end = System.currentTimeMillis();
        System.out.println("Unlocked access took millis: " + (end - start));
    }

    private void timeLockedAccess() {
        long start = System.currentTimeMillis();

        Runnable runnable = () -> {
            for (int j = 0; j < 100000; j++) {
                Optional<Integer> value = LockingWrapper.lockAndGetOptional(readLock, () -> Optional.ofNullable(map.get(random.nextInt())), "error");
            }
        };
        executeInParallel(runnable);
        long end = System.currentTimeMillis();
        System.out.println("Locked access took millis: " + (end - start));
    }

    private void executeInParallel(Runnable runnable) {
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Thread thread = new Thread(runnable);
            threadList.add(thread);
            thread.start();
        }
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
