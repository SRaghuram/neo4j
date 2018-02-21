/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.handlers.SecureClusteringPipelineFactory;

import org.neo4j.backup.impl.BackupModule;
import org.neo4j.backup.impl.BackupSupportingClassesFactory;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
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
    protected PipelineWrapper createPipelineWrapper( Config config )
    {
        SecureClusteringPipelineFactory factory = new SecureClusteringPipelineFactory();
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( SslPolicyLoader.create( config, logProvider ) );
        return factory.forServer( config, deps, logProvider );
    }
}

