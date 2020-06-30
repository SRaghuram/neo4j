/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;

import java.nio.file.Path;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;

public class CcRobustnessGraphDatabaseFactory extends DatabaseManagementServiceBuilder
{
    CcRobustnessGraphDatabaseFactory( Path homeDirectory )
    {
        super( homeDirectory );
    }

    @Override
    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory( Config config )
    {
        return globalModule -> new CoreEditionModule( globalModule, new AkkaDiscoveryServiceFactory() );
    }
}
