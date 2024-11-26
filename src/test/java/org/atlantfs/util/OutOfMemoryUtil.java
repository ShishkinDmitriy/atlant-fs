package org.atlantfs.util;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

public class OutOfMemoryUtil {

    private static final Logger log = Logger.getLogger(OutOfMemoryUtil.class.getName());

    static void trigger() {
        var ref = new SoftReference<>(new Object());
        while (ref.get() != null) {
            try {
                List<byte[]> list = new ArrayList<>();
                int index = 1;
                //noinspection InfiniteLoopStatement
                while (true) {
                    byte[] b = new byte[1_024 * 1_024]; // 1MB
                    list.add(b);
                    Runtime rt = Runtime.getRuntime();
                    log.fine("Iteration [index=" + index++ + ", listSize=" + list.size() + ", freeMemory=" + rt.freeMemory() + "]");
                }
            } catch (OutOfMemoryError e) {
                log.info("OutOfMemoryError caught");
            }
        }
        log.info("SoftReference cleared [value=" + ref.get() + "]");
        assertThat(ref.get()).isNull();
    }

}
