/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.handlers.SslServerPipelineWrapper;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.backup.impl.BackupModule;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommercialBackupSupportingClassesFactoryTest
{
    CommercialBackupSupportingClassesFactory subject;

    BackupModule backupModule = mock( BackupModule.class );

    OutsideWorld outsideWorld = mock( OutsideWorld.class );

    @Before
    public void setup()
    {
        when( backupModule.getOutsideWorld() ).thenReturn( outsideWorld );
        when( backupModule.getLogProvider() ).thenReturn( NullLogProvider.getInstance() );
        subject = new CommercialBackupSupportingClassesFactory( backupModule );
    }

    @Test
    public void commercialFactoryReturnsSslProviders()
    {
        Config config = Config.defaults();
        PipelineWrapper appender = subject.createPipelineWrapper( config );
        assertEquals( SslServerPipelineWrapper.class, appender.getClass() );
    }
}
