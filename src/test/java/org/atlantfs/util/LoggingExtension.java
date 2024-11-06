package org.atlantfs.util;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.logging.Logger;

public class LoggingExtension implements BeforeEachCallback, AfterEachCallback {

    private static final Logger log = Logger.getLogger(LoggingExtension.class.getName());

    @Override
    public void beforeEach(ExtensionContext context) {
        log.info(() -> String.format("Started  %s::%s %s", className(context), methodName(context), testName(context)));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        log.info(() -> String.format("Finished %s::%s %s", className(context), methodName(context), testName(context)));
    }

    private static String className(ExtensionContext context) {
        return context.getTestClass()
                .map(Class::getSimpleName)
                .orElse("");
    }

    private static String methodName(ExtensionContext context) {
        return context.getTestMethod()
                .map(Method::getName)
                .orElse("");
    }

    private static String testName(ExtensionContext context) {
        return context.getDisplayName();
    }

}
