/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.helper.Workload;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.stresstests.TxHelp.isInterrupted;
import static com.neo4j.causalclustering.stresstests.TxHelp.isTransient;

/**
 * Resources for stress testing ID-reuse scenarios.
 */
class IdReuse
{
    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName( "testType" );

    /**
     * Validate free ids. All must be unique.
     */
    static class UniqueFreeIds extends Validation
    {
        private final Cluster cluster;
        private final FileSystemAbstraction fs;
        private final Log log;

        UniqueFreeIds( Resources resources )
        {
            super();
            this.cluster = resources.cluster();
            this.fs = resources.fileSystem();
            this.log = resources.logProvider().getLog( getClass() );
        }

        @Override
        protected void validate()
        {
            Iterable<ClusterMember> members = Iterables.concat( cluster.coreMembers(), cluster.readReplicas() );
            Map<MemberId, Long> usedIdsPerMember = new HashMap<>();

            for ( ClusterMember member: members )
            {
                usedIdsPerMember.put( member.id(),
                        member.defaultDatabase().getDependencyResolver().resolveDependency(
                                        IdGeneratorFactory.class ).get( IdType.NODE ).getNumberOfIdsInUse() );
            }

            if ( usedIdsPerMember.values().stream().distinct().count() != 1 )
            {
                log.error( "Mismatching used id count found in cluster members: %s", usedIdsPerMember );
                throw new IllegalStateException( "Members don't have the same used id count" );
            }

            log.info( "Total of " + usedIdsPerMember.values().iterator().next() + " used ids found" );
        }
    }

    static class IdReuseSetup extends Preparation
    {
        private final Cluster cluster;

        IdReuseSetup( Resources resources )
        {
            super();
            cluster = resources.cluster();
        }

        @Override
        protected void prepare() throws Exception
        {
            for ( int i = 0; i < 1_000; i++ )
            {
                try
                {
                    cluster.coreTx( ( db, tx ) -> {
                        for ( int j = 0; j < 1_000; j++ )
                        {
                            Node start = db.createNode();
                            Node end = db.createNode();
                            start.createRelationshipTo( end, RELATIONSHIP_TYPE );
                        }
                        tx.commit();
                    } );
                }
                catch ( WriteOperationsNotAllowedException e )
                {
                    // skip
                }
            }
        }
    }

    static class InsertionWorkload extends Workload
    {
        private Cluster cluster;

        InsertionWorkload( Control control, Resources resources )
        {
            super( control );
            this.cluster = resources.cluster();
        }

        @Override
        protected void doWork()
        {
            try
            {
                cluster.coreTx( ( db, tx ) -> {
                    Node nodeStart = db.createNode();
                    Node nodeEnd = db.createNode();
                    nodeStart.createRelationshipTo( nodeEnd, RELATIONSHIP_TYPE );
                    tx.commit();
                } );
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "InsertionWorkload", e );
            }
        }
    }

    static class ReelectionWorkload extends Workload
    {
        private final long reelectIntervalSeconds;
        private final Log log;
        private Cluster cluster;

        ReelectionWorkload( Control control, Resources resources, Config config )
        {
            super( control );
            this.cluster = resources.cluster();
            this.reelectIntervalSeconds = config.reelectIntervalSeconds();
            this.log = config.logProvider().getLog( getClass() );
        }

        @Override
        protected void doWork()
        {
            try
            {
                CoreClusterMember leader = cluster.awaitLeader();
                leader.shutdown();
                leader.start();
                log.info( "Restarting leader" );
                TimeUnit.SECONDS.sleep( reelectIntervalSeconds );
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "ReelectionWorkload", e );
            }
        }
    }

    static class DeletionWorkload extends Workload
    {
        private final SecureRandom rnd = new SecureRandom();
        private final int idHighRange;
        private Cluster cluster;

        DeletionWorkload( Control control, Resources resources )
        {
            super( control );
            this.cluster = resources.cluster();
            this.idHighRange = 2_000_000;
        }

        @Override
        protected void doWork()
        {
            try
            {
                cluster.coreTx( ( db, tx ) -> {
                    Node node = db.getNodeById( rnd.nextInt( idHighRange ) );
                    Iterables.stream( node.getRelationships() ).forEach( Relationship::delete );
                    node.delete();

                    tx.commit();
                } );
            }
            catch ( NotFoundException e )
            {
                // Expected
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "DeletionWorkload", e );
            }
        }
    }
}
