/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import java.util.function.Consumer;

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.PrintStreamLogger;

public class StdoutLogProvider implements LogProvider
{
    @Override
    public Log getLog( Class<?> loggingClass )
    {
        return getLog( loggingClass.getSimpleName() );
    }

    @Override
    public Log getLog( String name )
    {
        PrefixingLogger logger = new PrefixingLogger( name, new PrintStreamLogger( System.out ) );
        return new SimpleLog(
                new PrefixingLogger( "DBG", logger ),
                new PrefixingLogger( "INF", logger ),
                new PrefixingLogger( "WRN", logger ),
                new PrefixingLogger( "ERR", logger )
        );
    }

    static class PrefixingLogger implements Logger
    {
        private final String prefix;
        private final Logger logger;

        PrefixingLogger( String prefix, Logger logger )
        {
            this.prefix = prefix;
            this.logger = logger;
        }

        @Override
        public void log( String message )
        {
            logger.log( prefix + " - " + message );
        }

        @Override
        public void log( String message, Throwable throwable )
        {
            logger.log( prefix + " - " + message, throwable );
        }

        @Override
        public void log( String format, Object... arguments )
        {
            logger.log( prefix + " - " + format, arguments );
        }

        @Override
        public void bulk( Consumer<Logger> consumer )
        {
            consumer.accept( this );
        }
    }

    static class SimpleLog extends AbstractLog
    {

        private final Logger debugLogger;
        private final Logger infoLogger;
        private final Logger warnLogger;
        private final Logger errorLogger;

        SimpleLog( Logger debugLogger, Logger infoLogger, Logger warnLogger, Logger errorLogger )
        {
            this.debugLogger = debugLogger;
            this.infoLogger = infoLogger;
            this.warnLogger = warnLogger;
            this.errorLogger = errorLogger;
        }

        @Override
        public boolean isDebugEnabled()
        {
            return true;
        }

        @Override
        public Logger debugLogger()
        {
            return debugLogger;
        }

        @Override
        public Logger infoLogger()
        {
            return infoLogger;
        }

        @Override
        public Logger warnLogger()
        {
            return warnLogger;
        }

        @Override
        public Logger errorLogger()
        {
            return errorLogger;
        }

        @Override
        public void bulk( Consumer<Log> consumer )
        {
            consumer.accept( this );
        }
    }
}
