/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.causalclustering.handlers.PipelineWrapper;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.ssl.config.SslPolicyLoader;

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
        deps.satisfyDependencies( SslPolicyLoader.create( config, logProvider ) );
        return factory.forClient( config, deps, logProvider, OnlineBackupSettings.ssl_policy );
    }
}

