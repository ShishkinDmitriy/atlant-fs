package org.atlantfs;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class Cache<K, V> {

    static class NoOp<K, V> extends Cache<K, V> {
        @Override
        V computeIfAbsent(K key, Function<? super K, ? extends V> remappingFunction) {
            return remappingFunction.apply(key);
        }

        @Override
        void put(K key, V inode) {
            // Do nothing
        }

        @Override
        void remove(K key) {
            // Do nothing
        }
    }

    private final Map<K, SoftReference<V>> cache = new ConcurrentHashMap<>();

    V computeIfAbsent(K key, Function<? super K, ? extends V> remappingFunction) {
        Object[] hardRefs = new Object[1];
        cache.compute(key, (id, ref) -> {
            if (ref != null) {
                var value = ref.get();
                if (value != null) {
                    hardRefs[0] = value;
                    return ref;
                }
            }
            var result = remappingFunction.apply(id);
            hardRefs[0] = result;
            return new SoftReference<>(result);
        });
        assert hardRefs[0] != null;
        //noinspection unchecked
        return (V) hardRefs[0];
    }

    void put(K key, V inode) {
        cache.put(key, new SoftReference<>(inode));
    }

    void remove(K key) {
        cache.remove(key);
    }

}
