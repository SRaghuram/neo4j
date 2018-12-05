/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@ExtendWith( SuppressOutputExtension.class )
class BackupOutputMonitorTest
{
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void receivingStoreFilesMessageCorrect()
    {
        // given
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new BackupOutputMonitor( System.out ) );

        // when
        StoreCopyClientMonitor storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();

        // then
        assertThat( suppressOutput.getOutputVoice().toString(), containsString( "Start receiving store files" ) );
    }
}
