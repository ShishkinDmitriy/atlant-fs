package org.atlantfs;

import java.nio.ByteBuffer;

interface Block {

//    private Id id;
//    private int length;
//    private T content;
//    private Class<T> type;
//
//    public Block(Id id, int length, T content, Class<T> type) {
//        this.id = id;
//        this.length = length;
//        this.content = content;
//        this.type = type;
//    }

    record Id(int value) implements AbstractId {

        static final int LENGTH = 4;

        static final Id ZERO = new Id(0);

        static Id of(int value) {
            return new Id(value);
        }

        static Id read(ByteBuffer buffer) {
            var address = buffer.getInt();
            return Id.of(address);
        }

        public void write(ByteBuffer buffer) {
            buffer.putInt(value);
        }

        Id plus(int val) {
            return new Id(value + val);
        }

        Id minus(Id val) {
            return new Id(value - val.value);
        }

        Id minus(int val) {
            return new Id(value - val);
        }

        boolean lessThan(Id another) {
            return value < another.value;
        }

        boolean greaterThan(Id another) {
            return value > another.value;
        }

        @Override
        public String toString() {
            return "Block.Id{" +
                    "value=" + value +
                    '}';
        }

    }

    record Range(Id from, int length) implements AbstractRange<Id> {

        static Range of(Id from, int length) {
            assert from.value >= 0;
            assert length > 0;
            return new Range(from, length);
        }

        @Override
        public String toString() {
            return "Block.Range{" +
                    "from=" + from +
                    ", length=" + length +
                    '}';
        }

    }

}
