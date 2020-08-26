/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.helper.Workload;

import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.TransactionIdStore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.function.Predicates.awaitForever;

class CatchupNewReadReplica extends Workload
{
    private static final long SAMPLE_INTERVAL_MS = 2000;
    private static final long MAX_LAG_MS = 500;

    private final FileSystemAbstraction fs;
    private final Log log;
    private final Cluster cluster;
    private boolean deleteStore;

    CatchupNewReadReplica( Control control, Resources resources )
    {
        super( control );
        this.fs = resources.fileSystem();
        this.cluster = resources.cluster();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    protected void doWork() throws IOException
    {
        int newMemberId = cluster.readReplicas().size();
        final ReadReplica readReplica = cluster.addReadReplicaWithId( newMemberId );

        log.info( "Adding " + readReplica );
        readReplica.start();

        LagEvaluator lagEvaluator = new LagEvaluator( this::leaderTxId, () -> txId( readReplica ), Clock.systemUTC() );

        awaitForever( () ->
        {
            if ( !control.keepGoing() )
            {
                return true;
            }

            Optional<LagEvaluator.Lag> lagEstimate = lagEvaluator.evaluate();

            if ( lagEstimate.isPresent() )
            {
                log.info( lagEstimate.get().toString() );
                return lagEstimate.get().timeLagMillis() < MAX_LAG_MS;
            }
            else
            {
                log.info( "Lag estimate not available" );
                return false;
            }
        }, SAMPLE_INTERVAL_MS, MILLISECONDS );

        if ( !control.keepGoing() )
        {
            return;
        }

        log.info( "Caught up" );
        cluster.removeReadReplicaWithMemberId( newMemberId );

        if ( deleteStore )
        {
            log.info( "Deleting store of " + readReplica );
            fs.deleteRecursively( readReplica.databaseLayout().databaseDirectory() );
        }
        deleteStore = !deleteStore;
   }

    private OptionalLong leaderTxId()
    {
        try
        {
            return txId( cluster.awaitLeader() );
        }
        catch ( TimeoutException e )
        {
            return OptionalLong.empty();
        }
    }

    private OptionalLong txId( ClusterMember member )
    {
        try
        {
            GraphDatabaseAPI database = member.defaultDatabase();
            TransactionIdStore txIdStore = database.getDependencyResolver().resolveDependency( TransactionIdStore.class );
            return OptionalLong.of( txIdStore.getLastClosedTransactionId() );
        }
        catch ( Throwable ex )
        {
            return OptionalLong.empty();
        }
    }
}
