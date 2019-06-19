/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.common.ClusterMonitors;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

public class ReadReplicaDatabaseManager extends ClusteredMultiDatabaseManager
{
    protected final ReadReplicaEditionModule edition;

    ReadReplicaDatabaseManager( GlobalModule globalModule, ReadReplicaEditionModule edition, Log log, CatchupComponentsFactory catchupComponentsFactory,
            FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config )
    {
        super( globalModule, edition, log, catchupComponentsFactory, fs, pageCache, logProvider, config );
        this.edition = edition;
    }

    @Override
    protected ClusteredDatabaseContext createDatabaseContext( DatabaseId databaseId )
    {
        // TODO: Remove need for resolving these dependencies? Remove internal operator?
        ClusterInternalDbmsOperator internalDbmsOperator = globalModule.getGlobalDependencies().resolveDependency( ClusterInternalDbmsOperator.class );

        Dependencies readReplicaDependencies = new Dependencies( globalModule.getGlobalDependencies() );
        Monitors readReplicaMonitors = ClusterMonitors.create( globalModule.getGlobalMonitors(), readReplicaDependencies );

        DatabaseCreationContext databaseCreationContext = newDatabaseCreationContext( databaseId, readReplicaDependencies, readReplicaMonitors );
        Database kernelDatabase = new Database( databaseCreationContext );

        LogFiles transactionLogs = buildTransactionLogs( kernelDatabase.getDatabaseLayout() );
        ReadReplicaDatabaseContext databaseContext = new ReadReplicaDatabaseContext( kernelDatabase, readReplicaMonitors, readReplicaDependencies, storeFiles,
                transactionLogs, internalDbmsOperator );

        ReadReplicaDatabaseLife readReplicaDatabase = edition.readReplicaDatabaseFactory().createDatabase( databaseContext, internalDbmsOperator );

        return contextFactory.create( kernelDatabase, kernelDatabase.getDatabaseFacade(), transactionLogs, storeFiles, logProvider, catchupComponentsFactory,
                readReplicaDatabase, readReplicaMonitors );
    }
}
