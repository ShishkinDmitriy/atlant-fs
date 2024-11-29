package org.atlantfs;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.IntStream;

interface Block {

    Id id();

    boolean isDirty();

    void flush();

    void delete() throws IOException;

    record Id(int value) implements org.atlantfs.Id {

        private static final Logger log = Logger.getLogger(Block.Id.class.getName());

        static final int LENGTH = 4;

        static final Id ZERO = new Id(0);

        static Id of(int value) {
            return new Id(value);
        }

        static Id read(ByteBuffer buffer) {
            var address = buffer.getInt();
            if (address < 0) {
                var position = buffer.position();
                buffer.clear();
                var array = new byte[buffer.capacity()];
                for (int i = 0; i < buffer.remaining(); i++) {
                    array[i] = buffer.get();
                }
                buffer.position(position);
                log.warning(() -> "Received negative block id [address=" + address + ", buffer=" + HexFormat.ofDelimiter(" ").formatHex(array) + "]");
                throw new RuntimeException();
            }
            return Block.Id.of(address);
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

    class Pointer<B extends Block> {

        private final Id id;
        private SoftReference<B> reference;

        public Pointer(Id id) {
            this.id = id;
            this.reference = new SoftReference<>(null);
        }

        public Pointer(B value) {
            this.id = value.id();
            this.reference = new SoftReference<>(value);
        }

        static <B extends Block> Pointer<B> of(Id id) {
            return new Pointer<>(id);
        }

        static <B extends Block> Pointer<B> of(B value) {
            return new Pointer<>(value);
        }

        void flush(ByteBuffer buffer) {
            id.write(buffer);
        }

        Id id() {
            return id;
        }

        B computeIfAbsent(Function<Id, B> reader) {
            var result = reference.get();
            if (result != null) {
                return result;
            }
            result = reader.apply(id);
            reference = new SoftReference<>(result);
            return result;
        }

        @Override
        public String toString() {
            return "Block.Pointer{" +
                    "id=" + id.value +
                    ", reference=" + reference.get() +
                    '}';
        }

    }

    record Range(Id from, int length) implements org.atlantfs.Range<Id> {

        static Range of(Id from, int length) {
            assert from.value >= 0;
            assert length > 0;
            return new Range(from, length);
        }

        static List<Id> flat(List<Range> ranges) {
            return ranges.stream()
                    .flatMap(range -> IntStream.range(0, range.length())
                            .mapToObj(i -> range.from().plus(i)))
                    .toList();
        }

        @Override
        public String toString() {
            return "Block.Range{" +
                    "from=" + from.value +
                    ", length=" + length +
                    '}';
        }

    }

}
