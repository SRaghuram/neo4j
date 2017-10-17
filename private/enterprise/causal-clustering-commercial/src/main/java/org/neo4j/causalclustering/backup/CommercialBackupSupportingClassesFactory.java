/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.backup;

import org.neo4j.backup.AbstractBackupSupportingClassesFactory;
import org.neo4j.backup.BackupModuleResolveAtRuntime;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppenderFactory;
import org.neo4j.causalclustering.handlers.SslPipelineHandlerAppenderFactory;

public class CommercialBackupSupportingClassesFactory extends AbstractBackupSupportingClassesFactory
{
    CommercialBackupSupportingClassesFactory( BackupModuleResolveAtRuntime backupModuleResolveAtRuntime )
    {
        super( backupModuleResolveAtRuntime );
    }

    @Override
    protected PipelineHandlerAppenderFactory getPipelineHandlerAppenderFactory()
    {
        return new SslPipelineHandlerAppenderFactory();
    }
}

