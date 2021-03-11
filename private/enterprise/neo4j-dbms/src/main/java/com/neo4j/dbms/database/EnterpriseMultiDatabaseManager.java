/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.dbms.TopologyPublisher;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;

public class EnterpriseMultiDatabaseManager extends MultiDatabaseManager<StandaloneEnterpriseDatabaseContext>
{
    private final TopologyPublisher.Factory topologyPublisherFactory;

    public EnterpriseMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition,
            TopologyPublisher.Factory topologyPublisherFactory )
    {
        super( globalModule, edition );
        this.topologyPublisherFactory = topologyPublisherFactory;
    }

    @Override
    protected StandaloneEnterpriseDatabaseContext createDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        var databaseCreationContext = newDatabaseCreationContext( namedDatabaseId, globalModule.getGlobalDependencies(), globalModule.getGlobalMonitors() );
        var kernelDatabase = new Database( databaseCreationContext );
        var enterpriseDatabase = EnterpriseDatabase.builder( EnterpriseDatabase::new )
                .withComponent( topologyPublisherFactory.apply( namedDatabaseId ) )
                .withKernelDatabase( kernelDatabase )
                .build();
        return new StandaloneEnterpriseDatabaseContext( kernelDatabase, enterpriseDatabase );
    }
}
