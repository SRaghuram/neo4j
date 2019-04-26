/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;
import static org.hamcrest.Matchers.equalToIgnoringCase;

public class RaftMonitorTest
{
    @Test
    public void shouldNotDuplicateToAnyLog()
    {
        AssertableLogProvider user = new AssertableLogProvider();
        AssertableLogProvider debug = new AssertableLogProvider();

        SimpleLogService logService = new SimpleLogService( user, debug );

        Monitors monitors = new Monitors();
        RaftMonitor.register( logService.getInternalLogProvider(), logService.getUserLogProvider(), monitors );

        RaftBinder.Monitor monitor = monitors.newMonitor( RaftBinder.Monitor.class );

        RaftId raftId = new RaftId( UUID.randomUUID() );
        DatabaseId databaseId = new DatabaseId( "smurf" );
        monitor.boundToRaft( databaseId, raftId );

        Matcher<String> expected = equalToIgnoringCase( format( "Bound database '%s' to raft with id '%s'.", databaseId.name(), raftId.uuid() ) );
        user.assertContainsExactlyOneMessageMatching( expected );
        debug.assertContainsExactlyOneMessageMatching( expected );
    }
}
