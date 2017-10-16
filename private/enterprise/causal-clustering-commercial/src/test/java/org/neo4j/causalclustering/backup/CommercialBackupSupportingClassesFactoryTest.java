/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.backup;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.backup.BackupModuleResolveAtRuntime;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppenderFactory;
import org.neo4j.causalclustering.handlers.SslPipelineHandlerAppenderFactory;
import org.neo4j.commandline.admin.OutsideWorld;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommercialBackupSupportingClassesFactoryTest
{
    CommercialBackupSupportingClassesFactory subject;

    BackupModuleResolveAtRuntime backupModuleResolveAtRuntime = mock( BackupModuleResolveAtRuntime.class );

    OutsideWorld outsideWorld = mock( OutsideWorld.class );

    @Before
    public void setup()
    {
        when( backupModuleResolveAtRuntime.getOutsideWorld() ).thenReturn( outsideWorld );
        subject = new CommercialBackupSupportingClassesFactory( backupModuleResolveAtRuntime );
    }

    @Test
    public void commercialFactoryReturnsSslProviders()
    {
        PipelineHandlerAppenderFactory pipelineHandlerAppenderFactory = subject.getPipelineHandlerAppenderFactory();
        assertEquals( SslPipelineHandlerAppenderFactory.class, pipelineHandlerAppenderFactory.getClass() );
    }
}
