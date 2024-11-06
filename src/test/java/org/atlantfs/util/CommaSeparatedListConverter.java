package org.atlantfs.util;

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.ArgumentConverter;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;

public class CommaSeparatedListConverter implements ArgumentConverter {

    @Override
    public final Object convert(Object source, ParameterContext context) throws ArgumentConversionException {
        if (source == null) {
            return null;
        }
        Class<?> generic = (Class<?>) ((ParameterizedType) context.getParameter().getParameterizedType()).getActualTypeArguments()[0];
        String[] split = source.toString().split(",");
        if (generic.equals(Integer.class)) {
            return Arrays.stream(split)
                    .map(String::trim)
                    .map(Integer::parseInt)
                    .toList();
        } else if (generic.equals(Short.class)) {
            return Arrays.stream(split)
                    .map(String::trim)
                    .map(Short::parseShort)
                    .toList();
        } else if (generic.equals(String.class)) {
            return Arrays.stream(split)
                    .map(String::trim)
                    .toList();
        } else if (generic.equals(Boolean.class)) {
            return Arrays.stream(split)
                    .map(String::trim)
                    .map(val -> switch (val) {
                        case "0" -> Boolean.FALSE;
                        case "1" -> Boolean.TRUE;
                        default -> Boolean.parseBoolean(val);
                    })
                    .toList();
        }
        throw new IllegalArgumentException("Unsupported generic type: " + generic);
    }

}
