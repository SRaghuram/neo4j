/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.File;
import java.util.UUID;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class ReadReplicaGraphDatabase extends GraphDatabaseFacade
{

    public interface ReadReplicaEditionModuleFactory
    {
        ClusteringEditionModule create( GlobalModule globalModule, DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId );
    }

    public ReadReplicaGraphDatabase( File storeDir, Config config, ExternalDependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory, ReadReplicaEditionModuleFactory editionModuleFactory )
    {
        this( storeDir, config, dependencies, discoveryServiceFactory, new MemberId( UUID.randomUUID() ), editionModuleFactory );
    }

    public ReadReplicaGraphDatabase( File storeDir, Config config, ExternalDependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId, ReadReplicaEditionModuleFactory editionModuleFactory )
    {
        Function<GlobalModule,AbstractEditionModule> factory =
                globalModule -> editionModuleFactory.create( globalModule, discoveryServiceFactory, memberId );
        new GraphDatabaseFacadeFactory( DatabaseInfo.READ_REPLICA, factory ).initFacade( storeDir, config,
                dependencies, this );
    }
}
