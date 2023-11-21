package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Contains wrapper methods for safe locking and unlocking (so we dont forget to unlock after locking)
 */
public class LockingWrapper {

    private static final Logger log = Logger.setup(LockingWrapper.class);

    public static <T> Optional<T> lockAndGetOptional(ReentrantReadWriteLock.ReadLock readLock, Supplier<Optional<T>> supplier, String errMsg, Object ... errMsgDetails) {
        try {
            readLock.lock();
            return supplier.get();
        } catch (Exception e) {
            log.error(errMsg, errMsgDetails, e);
        } finally {
            readLock.unlock();
        }
        return Optional.empty();
    }

    public static <T> Collection<T> lockAndGetCollection(ReentrantReadWriteLock.ReadLock readLock, Supplier<Collection<T>> supplier, String errMsg, Object ... errMsgDetails) {
        try {
            readLock.lock();
            return supplier.get();
        } catch (Exception e) {
            log.error(errMsg, errMsgDetails, e);
        } finally {
            readLock.unlock();
        }
        return Collections.emptyList();
    }

    public static <T> List<T> lockAndGetList(ReentrantReadWriteLock.ReadLock readLock, Supplier<List<T>> supplier, String errMsg, Object ... errMsgDetails) {
        try {
            readLock.lock();
            return supplier.get();
        } catch (Exception e) {
            log.error(errMsg, errMsgDetails, e);
        } finally {
            readLock.unlock();
        }
        return Collections.emptyList();
    }

    public static <K, T> Map<K, Map<K, T>> lockAndGetNestedMap(ReentrantReadWriteLock.ReadLock readLock, Supplier<Map<K, Map<K, T>>> supplier, String errMsg, Object ... errMsgDetails) {
        try {
            readLock.lock();
            return supplier.get();
        } catch (Exception e) {
            log.error(errMsg, errMsgDetails, e);
        } finally {
            readLock.unlock();
        }
        return Collections.emptyMap();
    }

    public static boolean lockAndWriteAndGetBoolean(ReentrantReadWriteLock.WriteLock writeLock, Supplier<Boolean> supplier, String errMsg, Object ... errMsgDetails) {
        try {
            writeLock.lock();
            return supplier.get();
        } catch (Exception e) {
            log.error(errMsg, errMsgDetails, e);
        } finally {
            writeLock.unlock();
        }
        return false;
    }

    public static <T> void lockAndWrite(ReentrantReadWriteLock.WriteLock writeLock, Consumer<T> consumer, T data, String errMsg, Object ... errMsgDetails) {
        try {
            writeLock.lock();
            consumer.accept(data);
        } catch (Exception e) {
            log.error(errMsg, errMsgDetails, e);
        } finally {
            writeLock.unlock();
        }
    }
}
