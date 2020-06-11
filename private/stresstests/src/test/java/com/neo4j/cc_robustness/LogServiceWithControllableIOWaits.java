/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.AbstractLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.AbstractLogService;
import org.neo4j.logging.internal.LogService;

import static java.lang.String.format;

/**
 * {@link LogService} which wraps another log service and which can add slowness to specific log messages.
 */
public class LogServiceWithControllableIOWaits extends AbstractLogService
{
    private static final Listener<String> NO_OP = notification ->
    {
    };

    private final LogService actual;
    private final Function<String,Listener<String>> waitFunction;

    LogServiceWithControllableIOWaits( LogService actual, final Listener<String> waiter, final Predicate<String> packagesAndClassesThatSeeAdditionalIOWaits )
    {
        this.actual = actual;
        this.waitFunction = classOrPackage ->
        {
            if ( packagesAndClassesThatSeeAdditionalIOWaits.test( classOrPackage ) )
            {
                return waiter;
            }
            return NO_OP;
        };
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return new WrappedLogProvider( actual.getUserLogProvider(), waitFunction );
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return new WrappedLogProvider( actual.getInternalLogProvider(), waitFunction );
    }

    private static class WrappedLogProvider extends AbstractLogProvider<Log>
    {
        private final LogProvider actual;
        private final Function<String,Listener<String>> waitFunction;

        WrappedLogProvider( LogProvider actual, Function<String,Listener<String>> waitFunction )
        {
            this.actual = actual;
            this.waitFunction = waitFunction;
        }

        @Override
        protected Log buildLog( Class loggingClass )
        {
            return new WrappedLog( actual.getLog( loggingClass ), waitFunction.apply( loggingClass.getName() ) );
        }

        @Override
        protected Log buildLog( String name )
        {
            return new WrappedLog( actual.getLog( name ), waitFunction.apply( name ) );
        }
    }

    private static class WrappedLog extends AbstractLog
    {
        private final Log actual;
        private final Listener<String> waiter;

        WrappedLog( Log actual, final Listener<String> waiter )
        {
            this.actual = actual;
            this.waiter = notification ->
            {
                if ( notification.contains( "uspicion" ) || notification.contains( "uspecting" ) )
                {
                    System.out.println( notification );
                }
                waiter.receive( notification );
            };
        }

        @Override
        public boolean isDebugEnabled()
        {
            return actual.isDebugEnabled();
        }

        @Override
        public Logger debugLogger()
        {
            return new WrappedLogger( actual.debugLogger(), waiter );
        }

        @Override
        public Logger infoLogger()
        {
            return new WrappedLogger( actual.infoLogger(), waiter );
        }

        @Override
        public Logger warnLogger()
        {
            return new WrappedLogger( actual.warnLogger(), waiter );
        }

        @Override
        public Logger errorLogger()
        {
            return new WrappedLogger( actual.errorLogger(), waiter );
        }

        @Override
        public void bulk( Consumer<Log> consumer )
        {
            consumer.accept( this );
        }
    }

    private static class WrappedLogger implements Logger
    {
        private final Logger actual;
        private final Listener<String> waiter;

        WrappedLogger( Logger actual, Listener<String> waiter )
        {
            this.actual = actual;
            this.waiter = waiter;
        }

        @Override
        public void log( String message )
        {
            waiter.receive( message );
            actual.log( message );
        }

        @Override
        public void log( String message, Throwable throwable )
        {
            waiter.receive( message + ": " + throwable );
            actual.log( message, throwable );
        }

        @Override
        public void log( String format, Object... arguments )
        {
            waiter.receive( format( format, arguments ) );
            actual.log( format, arguments );
        }

        @Override
        public void bulk( Consumer<Logger> consumer )
        {
            waiter.receive( "bulk" );
            actual.bulk( consumer );
        }
    }
}
