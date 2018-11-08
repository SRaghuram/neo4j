/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.ParameterisedOutsideWorld;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertTrue;

public class BackupOutputMonitorTest
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final Monitors monitors = new Monitors();
    private OutsideWorld outsideWorld;

    @Before
    public void setup()
    {
        outsideWorld = new ParameterisedOutsideWorld( System.console(), System.out, System.err, System.in, new DefaultFileSystemAbstraction() );
    }

    @Test
    public void receivingStoreFilesMessageCorrect()
    {
        // given
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld ) );

        // when
        StoreCopyClientMonitor storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();

        // then
        assertTrue( suppressOutput.getOutputVoice().toString().toString().contains( "Start receiving store files" ) );
    }
}
