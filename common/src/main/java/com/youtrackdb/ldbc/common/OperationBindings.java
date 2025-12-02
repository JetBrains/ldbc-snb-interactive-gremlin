package com.youtrackdb.ldbc.common;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OperationBindings {
    private static final Logger logger = LoggerFactory.getLogger(OperationBindings.class);

    public static <T extends Operation<?>> void bindQuery(
            Binder binder,
            Class<T> operationType,
            Class<? extends OperationHandler<T, TinkerPopConnectionState>> handlerType) {

        binder.bind(OperationHandler.class)
                .annotatedWith(Names.named(operationType.getSimpleName()))
                .to(handlerType)
                .in(Scopes.SINGLETON);

        logger.debug("Bound {} → {}", operationType.getSimpleName(), handlerType.getName());
    }
}
