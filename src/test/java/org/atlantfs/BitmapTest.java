package org.atlantfs;

import org.atlantfs.util.LoggingExtension;
import org.atlantfs.util.RandomUtil;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.ArgumentConverter;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.atlantfs.util.ByteBufferUtil.byteBuffer;

@ExtendWith(LoggingExtension.class)
class BitmapTest {

    private static final int BLOCK_SIZE = 8;

    //region Bitmap::reserve
    @CsvSource(value = {
            // Given           | Expected                |
            // bitset          | res  | bitset           |
            " 0000000000000000 |    0 | 0100000000000000 ",
            " 0100000000000000 |    1 | 0300000000000000 ",
            " 0200000000000000 |    0 | 0300000000000000 ",
            " 0300000000000000 |    2 | 0700000000000000 ",
            " fffffffffffffeff |   48 | ffffffffffffffff ",
            " ffffffffffffefff |   52 | ffffffffffffffff ",
            " fffffffffffffffe |   56 | ffffffffffffffff ",
            " ffffffffffffffef |   60 | ffffffffffffffff ",
    }, delimiter = '|')
    @ParameterizedTest
    void reserve_should_returnFirstFreeBit_when_requireSingle(String blocksHex, int expected, String expectedHex) {
        // Given
        var bitSet = BitSet.valueOf(byteBuffer(blocksHex));
        var bitmap = new Bitmap(bitSet, BLOCK_SIZE);
        // When
        var result = bitmap.reserve();
        // Then
        assertThat(result).isEqualTo(expected);
        assertThat(hexOf(bitmap)).isEqualTo(expectedHex);
    }

    @Test
    void reserve_should_returnNegative_when_notEnoughEmptyBits() {
        // Given
        var hex = "ffffffffffffffff";
        var bitSet = BitSet.valueOf(byteBuffer(hex));
        var bitmap = new Bitmap(bitSet, BLOCK_SIZE);
        // When
        var result = bitmap.reserve();
        // Then
        assertThat(result).isEqualTo(Bitmap.NOT_FOUND);
        assertThat(hexOf(bitmap)).isEqualTo(hex);
    }

    @CsvSource(value = {
            // Given           | Call | Expected                           |
            // bitset          | size | ranges          | bitset           |
            " 0000000000000000 |    1 |             0-1 | 0100000000000000", // 0000... -> 1000...
            " 0000000000000000 |    2 |             0-2 | 0300000000000000", // 0000... -> 1100...
            " 0000000000000000 |    3 |             0-3 | 0700000000000000", // 0000... -> 1110...
            " aa00000000000000 |    1 |             0-1 | ab00000000000000", // 0101 0101 0000... -> 1101 0101 0000...
            " aa00000000000000 |    2 |         0-1,2-1 | af00000000000000", // 0101 0101 0000... -> 1111 0101 0000...
            " aa00000000000000 |    4 | 0-1,2-1,4-1,6-1 | ff00000000000000", // 0101 0101 0000... -> 1111 1111 0000...
            " fffffffffffffff7 |    1 |            59-1 | ffffffffffffffff", // ...0111 -> ...1111
            " ffffffffffffff7f |    1 |            63-1 | ffffffffffffffff", // ...1110 -> ...1111
            " ffffffffffffff7f |    2 |            63-1 | ffffffffffffffff", // ...1110 -> ...1111
            " ffffffffffffff7f |   15 |            63-1 | ffffffffffffffff", // ...1110 -> ...1111
    }, delimiter = '|')
    @ParameterizedTest
    void reserve_should_returnListOfFreeRanges_when_requireMultiple(String blocksHex, int size, @ConvertWith(RangesListConverter.class) List<Bitmap.Range> expectedRanges, String expectedHex) {
        // Given
        var bitSet = BitSet.valueOf(byteBuffer(blocksHex));
        var bitmap = new Bitmap(bitSet, BLOCK_SIZE);
        // When
        var result = bitmap.reserve(size);
        // Then
        assertThat(result).containsExactlyElementsOf(expectedRanges);
        assertThat(hexOf(bitmap)).isEqualTo(expectedHex);
    }

    @Test
    void reserve_should_returnEmptyList_when_noEmptyRanges() {
        // Given
        var hex = "ffffffffffffffff";
        var bitSet = BitSet.valueOf(byteBuffer(hex));
        var bitmap = new Bitmap(bitSet, BLOCK_SIZE);
        // When
        var result = bitmap.reserve(1);
        // Then
        assertThat(result).isEmpty();
        assertThat(hexOf(bitmap)).isEqualTo(hex);
    }
    //endregion

    //region Bitmap::free
    @CsvSource(value = {
            // Given           | Call | Expected                  |
            // bitset          | del  |    res | bitset           |
            " ffffffffffffffff |    0 |   true | feffffffffffffff ",
            " ffffffffffffffff |    1 |   true | fdffffffffffffff ",
            " ffffffffffffffff |    2 |   true | fbffffffffffffff ",
            " ffffffffffffffff |    3 |   true | f7ffffffffffffff ",
            " ffffffffffffffff |   48 |   true | fffffffffffffeff ",
            " ffffffffffffffff |   52 |   true | ffffffffffffefff ",
            " ffffffffffffffff |   56 |   true | fffffffffffffffe ",
            " ffffffffffffffff |   60 |   true | ffffffffffffffef ",
            " ffffffffffffffff |   61 |   true | ffffffffffffffdf ",
            " ffffffffffffffff |   62 |   true | ffffffffffffffbf ",
            " ffffffffffffffff |   63 |   true | ffffffffffffff7f ",
            " ffffffffffffffff |   64 |  false | ffffffffffffffff ",
    }, delimiter = '|')
    @ParameterizedTest
    void free_should_unsetBit_when_single(String blocksHex, int bit, boolean expectedResult, String expectedHex) {
        // Given
        var bitSet = BitSet.valueOf(byteBuffer(blocksHex));
        var bitmap = new Bitmap(bitSet, BLOCK_SIZE);
        // When
        var result = bitmap.free(bit);
        // Then
        assertThat(result).isEqualTo(expectedResult);
        assertThat(hexOf(bitmap)).isEqualTo(expectedHex);
    }

    @CsvSource(value = {
            // Given           | Call | Expected         |
            // bitset          | del  | bitset           |
            " 0000000000000000 |  0-1 | 0000000000000000 ",
            " 0000000000000000 | 63-1 | 0000000000000000 ",
            " ab0000000000ff00 | 63-1 | ab0000000000ff00 ",
            " 0000000000000000 | 0-64 | 0000000000000000 ",
            " ffffffffffffffff |  0-1 | feffffffffffffff ",
            " ffffffffffffffff |  0-2 | fcffffffffffffff ",
            " ffffffffffffffff | 0-64 | 0000000000000000 ",
            " ffffffffffffffff | 63-1 | ffffffffffffff7f ",
            " aa00000000000000 |  0-8 | 0000000000000000 ",
    }, delimiter = '|')
    @ParameterizedTest
    void free_should_unsetAllBitsInRange_when_range(String blocksHex, @ConvertWith(RangeConverter.class) Bitmap.Range range, String expectedHex) {
        // Given
        var bitSet = BitSet.valueOf(byteBuffer(blocksHex));
        var bitmap = new Bitmap(bitSet, BLOCK_SIZE);
        // When
        bitmap.free(range);
        // Then
        assertThat(hexOf(bitmap)).isEqualTo(expectedHex);
    }
    //endregion

    //region Bitmap::reserve + Bitmap::free
    @TestFactory
    Stream<DynamicTest> reserveThenFree_should_retrieveTheSameResult() {
        return IntStream.range(0, 20)
                .mapToLong(_ -> RandomUtil.randomLong())
                .mapToObj(l -> BitSet.valueOf(new long[]{l}))
                .map(bitSet -> DynamicTest.dynamicTest(hexOf(bitSet), () -> {
                    // Given
                    var bitmap = new Bitmap(bitSet, BLOCK_SIZE);
                    // When
                    var bit = bitmap.reserve();
                    var result = bitmap.free(bit);
                    // Then
                    assertThat(result).isTrue();
                    assertThat(hexOf(bitmap)).isEqualTo(hexOf(bitSet));
                }));
    }
    //endregion

    private static String hexOf(Bitmap bitmap) {
        return hexOf(bitmap.getBitset());
    }

    private static String hexOf(BitSet bitset) {
        return String.format("%-16s", HexFormat.of().formatHex(bitset.toByteArray())).replace(' ', '0');
    }

    private static class RangesListConverter implements ArgumentConverter {

        private final RangeConverter rangeConverter = new RangeConverter();

        @Override
        public final Object convert(Object source, ParameterContext context) throws ArgumentConversionException {
            if (source == null) {
                return null;
            }
            return Arrays.stream(source.toString().split(","))
                    .map(String::trim)
                    .map(str -> rangeConverter.convert(str, context))
                    .toList();
        }

    }

    private static class RangeConverter implements ArgumentConverter {

        @Override
        public final Object convert(Object source, ParameterContext context) throws ArgumentConversionException {
            if (source == null) {
                return null;
            }
            var split = source.toString().split("-");
            assert split.length == 2;
            return Bitmap.Range.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
        }

    }

}
