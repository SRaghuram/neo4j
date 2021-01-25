/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.neo4j.logging.internal.LogService;

class SlowLogging implements UnaryOperator<LogService>
{
    private final RandomWaiting randomWaiting = new RandomWaiting();
    private final Predicate<String> affectedLoggers;

    SlowLogging( Map<String,String> parameters )
    {
        String slowLoggers = parameters.get( CcRobustness.SLOW_LOGGING_LOGGERS );
        if ( slowLoggers == null )
        {
            affectedLoggers = startsWithAnyOf( CcRobustness.DEFAULT_SLOW_LOGGING_LOGGERS );
        }
        else
        {
            slowLoggers = slowLoggers.trim();
            if ( slowLoggers.isEmpty() )
            {
                // no loggers are to be slowed down,
                // use null to signal that we don't even need to decorate the LogService
                affectedLoggers = null;
            }
            else
            {
                // the config is a comma separated list of logger name prefixes
                // logger names are typically class names, so by using prefixes we can denote packages
                affectedLoggers = startsWithAnyOf( slowLoggers.split( "," ) );
            }
        }
    }

    private static Predicate<String> startsWithAnyOf( final String... prefixes )
    {
        return string ->
        {
            for ( String prefix : prefixes )
            {
                if ( string.startsWith( prefix ) )
                {
                    return true;
                }
            }
            return false;
        };
    }

    void enable( float probability )
    {
        if ( affectedLoggers != null && probability > 0 )
        {
            System.out.println( "Slowing down logging with probability: " + probability );
            randomWaiting.enable( probability );
        }
    }

    @Override
    public LogService apply( LogService logService )
    {
        if ( affectedLoggers == null )
        {
            return logService;
        }
        return new LogServiceWithControllableIOWaits( logService, randomWaiting, affectedLoggers );
    }
}
