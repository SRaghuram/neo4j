/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.common.RaftMonitors;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;
import com.neo4j.dbms.database.DefaultClusteredDatabaseContext;

import java.io.IOException;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;

public class ReadReplicaDatabaseManager extends ClusteredMultiDatabaseManager
{
    protected final ReadReplicaEditionModule edition;

    ReadReplicaDatabaseManager( GlobalModule globalModule, ReadReplicaEditionModule edition, CatchupComponentsFactory catchupComponentsFactory,
            FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config, ClusterStateLayout clusterStateLayout )
    {
        super( globalModule, edition, catchupComponentsFactory, fs, pageCache, logProvider, config, clusterStateLayout );
        this.edition = edition;
    }

    @Override
    protected ClusteredDatabaseContext createDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        Dependencies readReplicaDependencies = new Dependencies( globalModule.getGlobalDependencies() );
        Monitors readReplicaMonitors = RaftMonitors.create( globalModule.getGlobalMonitors(), readReplicaDependencies );
        var pageCacheTracer = globalModule.getTracers().getPageCacheTracer();

        DatabaseCreationContext databaseCreationContext = newDatabaseCreationContext( namedDatabaseId, readReplicaDependencies, readReplicaMonitors );
        Database kernelDatabase = new Database( databaseCreationContext );

        LogFiles transactionLogs = buildTransactionLogs( kernelDatabase.getDatabaseLayout() );
        ReadReplicaDatabaseContext databaseContext = new ReadReplicaDatabaseContext( kernelDatabase, readReplicaMonitors, readReplicaDependencies, storeFiles,
                transactionLogs, internalDbmsOperator(), pageCacheTracer );

        ReadReplicaDatabase readReplicaDatabase = edition.readReplicaDatabaseFactory().createDatabase(
                databaseContext, internalDbmsOperator() );

        return DefaultClusteredDatabaseContext
                .readReplica( kernelDatabase, kernelDatabase.getDatabaseFacade(), transactionLogs, storeFiles, logProvider, catchupComponentsFactory,
                              readReplicaDatabase, readReplicaMonitors, pageCacheTracer );
    }

    @Override
    public void cleanupClusterState( String databaseName )
    {
        try
        {
            deleteRaftGroupId( databaseName );
        }
        catch ( IOException e )
        {
            throw new DatabaseManagementException( "Was unable to delete cluster state as part of drop. ", e );
        }
    }

    /**
     * When deleting a read replica we must subsequently clear out its raft id from the cluster state.
     * in extremis if some other IO failure occurs, the existence of a raft-id indicates that more cleanup may be required.
     */
    private void deleteRaftGroupId( String databaseName ) throws IOException
    {
        var raftGroupDir = clusterStateLayout.raftGroupDir( databaseName );
        var raftIdState = clusterStateLayout.raftGroupIdFile( databaseName );
        var raftIdStateDir = raftIdState.getParent();

        if ( !fs.deleteFile( raftIdState ) )
        {
            throw new IOException( format( "Unable to delete file %s when dropping database %s", raftIdState.toAbsolutePath(), databaseName ) );
        }

        FileUtils.tryForceDirectory( raftIdStateDir );

        fs.deleteRecursively( raftGroupDir );
    }
}
