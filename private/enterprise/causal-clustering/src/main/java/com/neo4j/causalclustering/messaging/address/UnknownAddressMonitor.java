/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.address;

import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.logging.Log;
import org.neo4j.logging.internal.CappedLogger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class UnknownAddressMonitor
{
    private final Log log;
    private final Clock clock;
    private final long timeLimitMs;
    private final Map<MemberId,CappedLogger> loggers = new ConcurrentHashMap<>();

    public UnknownAddressMonitor( Log log, Clock clock, long timeLimitMs )
    {
        this.log = log;
        this.clock = clock;
        this.timeLimitMs = timeLimitMs;
    }

    public void logAttemptToSendToMemberWithNoKnownAddress( MemberId to )
    {
        CappedLogger cappedLogger = loggers.get( to );
        if ( cappedLogger == null )
        {
            cappedLogger = new CappedLogger( log, timeLimitMs, MILLISECONDS, clock );
            loggers.put( to, cappedLogger );
        }
        cappedLogger.info( "No address found for %s, probably because the member has been shut down.", to );
    }
}
