/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.SslPipelineHandlerAppenderFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.util.Dependencies;

public class CommercialBackupSupportingClassesFactory extends BackupSupportingClassesFactory
{
    CommercialBackupSupportingClassesFactory( BackupModule backupModule )
    {
        super( backupModule );
    }

    @Override
    protected PipelineHandlerAppender createPipelineHandlerAppender( Config config )
    {
        SslPipelineHandlerAppenderFactory factory = new SslPipelineHandlerAppenderFactory();
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( SslPolicyLoader.create( config, logProvider ) );
        return factory.create( config, deps, logProvider );
    }
}

