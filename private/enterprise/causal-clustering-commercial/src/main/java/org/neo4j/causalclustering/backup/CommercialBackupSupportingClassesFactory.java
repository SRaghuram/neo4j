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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;

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

    @Override
    protected Dependencies getDependencies(LogProvider logProvider, Config config )
    {
        Dependencies dependencies = super.getDependencies( logProvider, config );
        dependencies.satisfyDependencies( SslPolicyLoader.create( config, logProvider ) );
        return dependencies;
    }
}

