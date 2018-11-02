/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.diagnostics;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.identity.ClusterBinder;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;

public class CoreMonitorTest
{

    @Test
    public void shouldNotDuplicateToAnyLog()
    {
        AssertableLogProvider user = new AssertableLogProvider();
        AssertableLogProvider debug = new AssertableLogProvider();

        SimpleLogService logService = new SimpleLogService( user, debug );

        Monitors monitors = new Monitors();
        CoreMonitor.register( logService.getInternalLogProvider(), logService.getUserLogProvider(), monitors );

        ClusterBinder.Monitor monitor = monitors.newMonitor( ClusterBinder.Monitor.class );

        ClusterId clusterId = new ClusterId( UUID.randomUUID() );
        monitor.boundToCluster( clusterId );

        user.assertContainsExactlyOneMessageMatching( Matchers.equalToIgnoringCase( "Bound to cluster with id " + clusterId.uuid() ) );
        debug.assertContainsExactlyOneMessageMatching( Matchers.equalToIgnoringCase( "Bound to cluster with id " + clusterId.uuid() ) );
    }
}
