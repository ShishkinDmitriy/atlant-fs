package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.List;

class DirTreeNode {

    private int length;
    private int depth;
    private List<DxEntry> dxEntries;

    static DirTreeNode read(ByteBuffer buffer) {
        return new DirTreeNode();
    }

    Block.Id get(String name) {
        var hash = name.hashCode();
        var firstHash = dxEntries.getFirst().hash;
        if (firstHash > hash) {
            throw new IllegalStateException("First hash [" + firstHash + "] of Dir tree node is greater than required [" + hash + "]");
        }
        for (int i = 0; i < dxEntries.size(); i++) {
            if (dxEntries.get(i).hash > hash) {
                return dxEntries.get(i - 1).block;
            }
        }
        return dxEntries.getLast().block();
    }

    void checkInvariant() {
        assert dxEntries != null;
        assert !dxEntries.isEmpty();
        assert isSorted();
    }

    boolean isSorted() {
        var last = dxEntries.getFirst().hash;
        for (int i = 1; i < dxEntries.size(); i++) {
            if (dxEntries.get(i).hash < last) {
                return false;
            }
        }
        return true;
    }

    // -----------------------------------------------------------------------------------------------------------------

    int getDepth() {
        return depth;
    }

    List<DxEntry> getDxEntries() {
        return dxEntries;
    }

    record DxEntry(int hash, Block.Id block) {

        static DxEntry read(ByteBuffer buffer) {
            var hash = buffer.getInt();
            var block = buffer.getInt();
            return new DxEntry(hash, Block.Id.of(block));
        }

        void write(ByteBuffer buffer) {
            buffer.putInt(hash);
            buffer.putInt(block().value());
        }

    }

}
