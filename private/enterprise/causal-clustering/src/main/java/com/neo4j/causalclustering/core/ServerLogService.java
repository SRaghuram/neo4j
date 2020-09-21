/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.AbstractLogService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.PrefixedLogProvider;
import org.neo4j.logging.internal.SimpleLogService;

public class ServerLogService extends AbstractLogService
{
    private final LogService delegate;
    private final String serverName;

    public ServerLogService( LogService delegate, String serverName )
    {
        this.delegate = delegate;
        this.serverName = serverName;
    }

    public ServerLogService( LogProvider debugLogProvider, LogProvider userLogProvider, String serverName )
    {
        this( new SimpleLogService( userLogProvider, debugLogProvider ), serverName );
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return new PrefixedLogProvider( delegate.getUserLogProvider(), serverName );
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return new PrefixedLogProvider( delegate.getInternalLogProvider(), serverName );
    }
}
