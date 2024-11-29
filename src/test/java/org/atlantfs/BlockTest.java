package org.atlantfs;

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.ArgumentConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class BlockTest {

    static class BlockIdListConverter implements ArgumentConverter {

        private final BlockIdConverter blockIdConverter = new BlockIdConverter();

        @Override
        public final List<Block.Id> convert(Object source, ParameterContext context) throws ArgumentConversionException {
            if (source == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(source.toString().split(","))
                    .map(String::trim)
                    .map(str -> blockIdConverter.convert(str, context))
                    .toList();
        }

    }

    static class BlockIdLis0fListConverter implements ArgumentConverter {

        private final BlockIdListConverter blockIdListConverter = new BlockIdListConverter();

        @Override
        public final List<List<Block.Id>> convert(Object source, ParameterContext context) throws ArgumentConversionException {
            if (source == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(source.toString().split(";"))
                    .map(String::trim)
                    .map(str -> blockIdListConverter.convert(str, context))
                    .toList();
        }

    }

    static class BlockIdConverter implements ArgumentConverter {

        @Override
        public final Block.Id convert(Object source, ParameterContext context) throws ArgumentConversionException {
            if (source == null) {
                return null;
            }
            return Block.Id.of(Integer.parseInt(source.toString()));
        }

    }

}
