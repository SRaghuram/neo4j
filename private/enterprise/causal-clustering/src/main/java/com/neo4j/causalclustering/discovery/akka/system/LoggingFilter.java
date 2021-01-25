/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorSystem;
import akka.event.DefaultLoggingFilter;
import akka.event.EventStream;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class LoggingFilter implements akka.event.LoggingFilter
{
    private static LogProvider logProvider = NullLogProvider.getInstance();
    private final DefaultLoggingFilter defaultLoggingFilter;

    static void enable( LogProvider logProvider )
    {
        LoggingFilter.logProvider = logProvider;
    }

    LoggingFilter( ActorSystem.Settings settings, EventStream eventStream )
    {
        defaultLoggingFilter = new DefaultLoggingFilter( settings, eventStream );
    }

    @Override
    public boolean isErrorEnabled( Class<?> logClass, String logSource )
    {
        return defaultLoggingFilter.isErrorEnabled( logClass, logSource );
    }

    @Override
    public boolean isWarningEnabled( Class<?> logClass, String logSource )
    {
        return defaultLoggingFilter.isWarningEnabled( logClass, logSource );
    }

    @Override
    public boolean isInfoEnabled( Class<?> logClass, String logSource )
    {
        return defaultLoggingFilter.isInfoEnabled( logClass, logSource );
    }

    @Override
    public boolean isDebugEnabled( Class<?> logClass, String logSource )
    {
        return defaultLoggingFilter.isDebugEnabled( logClass, logSource ) && logProvider.getLog( logClass ).isDebugEnabled();
    }
}
