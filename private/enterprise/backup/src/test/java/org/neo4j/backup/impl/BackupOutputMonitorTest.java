/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.Test;

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.containsString;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class BackupOutputMonitorTest
{
    @Test
    void receivingStoreFilesMessageCorrect()
    {
        // given
        Monitors monitors = new Monitors();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider ) );

        // when
        StoreCopyClientMonitor storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();

        // then
        logProvider.assertAtLeastOnce( inLog( BackupOutputMonitor.class ).info( containsString( "Start receiving store files" ) ) );
    }
}
