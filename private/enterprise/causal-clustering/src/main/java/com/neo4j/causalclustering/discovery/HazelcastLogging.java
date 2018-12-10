/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggerFactory;

import java.util.logging.Level;

import org.neo4j.logging.LogProvider;

public class HazelcastLogging implements LoggerFactory
{
    // there is no constant in the hazelcast library for this
    private static final String HZ_LOGGING_CLASS_PROPERTY = "hazelcast.logging.class";

    private static LogProvider logProvider;
    private static Level minLevel;

    static void enable( LogProvider logProvider, Level minLevel )
    {
        HazelcastLogging.logProvider = logProvider;
        HazelcastLogging.minLevel = minLevel;

        // hazelcast only allows configuring logging through system properties
        System.setProperty( HZ_LOGGING_CLASS_PROPERTY, HazelcastLogging.class.getName() );
    }

    @Override
    public ILogger getLogger( String name )
    {
        return new HazelcastLogger( logProvider.getLog( name ), minLevel );
    }
}
