/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.handlers.SecurePipelineFactory;

import org.neo4j.backup.impl.BackupModule;
import org.neo4j.backup.impl.BackupSupportingClassesFactory;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.configuration.ssl.TrustManagerFactoryProvider;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
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
        SecurePipelineFactory factory = new SecurePipelineFactory();
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( SslPolicyLoader.create( config, new TrustManagerFactoryProvider(), logProvider ) );
        return factory.forClient( config, deps, logProvider, OnlineBackupSettings.ssl_policy );
    }
}

