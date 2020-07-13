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
        public void debug( String message )
        {
            waiter.receive( message );
            actual.debug( message );
        }

        @Override
        public void debug( String message, Throwable throwable )
        {
            waiter.receive( message + ": " + throwable );
            actual.debug( message, throwable );
        }

        @Override
        public void debug( String format, Object... arguments )
        {
            waiter.receive( format( format, arguments ) );
            actual.debug( format, arguments );
        }

        @Override
        public void info( String message )
        {
            waiter.receive( message );
            actual.info( message );
        }

        @Override
        public void info( String message, Throwable throwable )
        {
            waiter.receive( message + ": " + throwable );
            actual.info( message, throwable );
        }

        @Override
        public void info( String format, Object... arguments )
        {
            waiter.receive( format( format, arguments ) );
            actual.info( format, arguments );
        }

        @Override
        public void warn( String message )
        {
            waiter.receive( message );
            actual.warn( message );
        }

        @Override
        public void warn( String message, Throwable throwable )
        {
            waiter.receive( message + ": " + throwable );
            actual.warn( message, throwable );
        }

        @Override
        public void warn( String format, Object... arguments )
        {
            waiter.receive( format( format, arguments ) );
            actual.warn( format, arguments );
        }

        @Override
        public void error( String message )
        {
            waiter.receive( message );
            actual.error( message );
        }

        @Override
        public void error( String message, Throwable throwable )
        {
            waiter.receive( message + ": " + throwable );
            actual.error( message, throwable );
        }

        @Override
        public void error( String format, Object... arguments )
        {
            waiter.receive( format( format, arguments ) );
            actual.error( format, arguments );
        }

        @Override
        public void bulk( Consumer<Log> consumer )
        {
            waiter.receive( "bulk" );
            actual.bulk( consumer );
        }
    }
}
