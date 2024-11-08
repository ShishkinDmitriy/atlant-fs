package org.atlantfs;

class Block<T> {

    private Id id;
    private int length;
    private T content;
    private Class<T> type;

    public Block(Id id, int length, T content, Class<T> type) {
        this.id = id;
        this.length = length;
        this.content = content;
        this.type = type;
    }

    record Id(int value) implements AbstractId {

        static final Id ZERO = new Id(0);

        static Id of(int value) {
            return new Id(value);
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
            return String.valueOf(value);
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
