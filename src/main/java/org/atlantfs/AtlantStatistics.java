package org.atlantfs;

import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

public class AtlantStatistics {

    private static final Logger log = Logger.getLogger(AtlantStatistics.class.getName());

    private final LongAdder readCalls = new LongAdder();
    private final LongAdder readBytes = new LongAdder();
    private final LongAdder writeCalls = new LongAdder();
    private final LongAdder writeBytes = new LongAdder();

    void incrementReadCalls() {
        readCalls.increment();
    }

    void incrementWriteCalls() {
        writeCalls.increment();
    }

    void addReadBytes(long bytes) {
        readBytes.add(bytes);
    }

    void addWriteBytes(long bytes) {
        writeBytes.add(bytes);
    }

    void print() {
        log.info(() -> "Statistics: [readCalls=" + readCalls.sum() + ", readBytes=" + readBytes.sum() + ", writeCalls=" + writeCalls.sum() + ", writeBytes=" + writeBytes.sum() + "]");
    }

}
