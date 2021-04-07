/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.helper.Workload;
import org.assertj.core.api.Condition;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.causalclustering.stresstests.TxHelp.isInterrupted;
import static com.neo4j.causalclustering.stresstests.TxHelp.isTransient;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * Resources for stress testing ID-reuse scenarios.
 */
class IdReuse
{
    /**
     * Validate free ids. All must be unique.
     */
    static class UniqueFreeIds extends Validation
    {
        private final Cluster cluster;
        private final Log log;

        UniqueFreeIds( Resources resources )
        {
            super();
            this.cluster = resources.cluster();
            this.log = resources.logProvider().getLog( getClass() );
        }

        @Override
        protected void validate() throws Exception
        {
            var members = cluster.allMembers();

            var lastTxId = getLastTxId( cluster.awaitLeader() );
            for ( ClusterMember member : members )
            {
                assertEventually( () -> getLastTxId( member ),
                                  new Condition<>( value -> value.equals( lastTxId ), "Last tx id condition." ),
                                  1, MINUTES );
            }

            var usedIdsPerMember = getUsedIdsPerMember( members );
            if ( usedIdsPerMember.values().stream().distinct().count() == 1 )
            {
                log.info( "Total of " + usedIdsPerMember.values().iterator().next() + " used ids found" );
                return;
            }
            log.warn( "Mismatching used id count found in cluster members: %s\nChecking highest written ids...", usedIdsPerMember );

            usedIdsPerMember = getWrittenIdsPerMember( members );
            if ( usedIdsPerMember.values().stream().distinct().count() == 1 )
            {
                log.info( "Total of " + usedIdsPerMember.values().iterator().next() + " written ids found" );
                return;
            }
            else
            {
                throw new IllegalStateException( "Members don't have the same used id count" );
            }
        }

        private Map<ServerId,Long> getUsedIdsPerMember( Set<ClusterMember> members )
        {
            Map<ServerId,Long> usedIdsPerMember = new HashMap<>();

            for ( ClusterMember member : members )
            {
                usedIdsPerMember.put( member.serverId(), member
                        .defaultDatabase()
                        .getDependencyResolver()
                        .resolveDependency( IdGeneratorFactory.class )
                        .get( IdType.NODE )
                        .getNumberOfIdsInUse() );
            }
            return usedIdsPerMember;
        }

        private Map<ServerId,Long> getWrittenIdsPerMember( Set<ClusterMember> members )
        {
            Map<ServerId,Long> usedIdsPerMember = new HashMap<>();

            for ( ClusterMember member: members )
            {
                usedIdsPerMember.put( member.serverId(), member
                        .defaultDatabase()
                        .getDependencyResolver()
                        .resolveDependency( IdGeneratorFactory.class )
                        .get( IdType.NODE )
                        .getHighestWritten() );
            }
            return usedIdsPerMember;
        }

        private long getLastTxId( ClusterMember member )
        {
            TransactionIdStore txIdStore = member.resolveDependency( DEFAULT_DATABASE_NAME, TransactionIdStore.class );
            return txIdStore.getLastClosedTransactionId();
        }

        @Override
        protected boolean postStop()
        {
            return false;
        }
    }

    static class IdReuseSetup extends Preparation
    {
        private final Resources resource;

        IdReuseSetup( Resources resources )
        {
            super();
            this.resource = resources;

        }

        @Override
        protected void prepare() throws Exception
        {
            try
            {
                for ( int i = 0; i < 1_000; i++ )
                {
                    try ( Session session = resource.driver().session( SessionConfig.builder().build() ) )
                    {
                        for ( int j = 0; j < 1_000; j++ )
                        {
                            var firstIndex = resource.getNextNodeIndex();
                            var secondIndex = resource.getNextNodeIndex();
                            Map<String,Object> parameters = Map.of( "id1", firstIndex, "id2", secondIndex );
                            createTwoNodesWithRelationship( session, parameters );
                        }
                    }
                }
            }
            catch ( DatabaseException e )
            {
                // skip
            }
        }
    }

    static class InsertionWorkload extends Workload
    {
        private final Resources resource;

        InsertionWorkload( Control control, Resources resources )
        {
            super( control );
            this.resource = resources;
        }

        @Override
        protected void doWork()
        {
            try ( Session session = resource.driver().session( SessionConfig.builder().build() ) )
            {
                var firstIndex = resource.getNextNodeIndex();
                var secondIndex = resource.getNextNodeIndex();
                Map<String,Object> parameters = Map.of( "id1", firstIndex, "id2", secondIndex );
                createTwoNodesWithRelationship( session, parameters );
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
        private final Cluster cluster;

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
        private final Driver driver;

        DeletionWorkload( Control control, Driver driver )
        {
            super( control );
            this.driver = driver;
            this.idHighRange = 2_000_000;
        }

        @Override
        protected void doWork()
        {
            try ( Session session = driver.session( SessionConfig.builder().build() ) )
            {
                int nodeIndex = rnd.nextInt( idHighRange );
                //delete node and all relationships
                session.writeTransaction( tx -> tx.run( "MATCH (n {id: $id}) DETACH DELETE n", Map.of( "id", nodeIndex ) ) ).consume();
            }
            catch ( NoSuchRecordException e )
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

    private static void createTwoNodesWithRelationship( Session session, Map<String,Object> parameters )
    {
        session.writeTransaction( tx -> tx.run( "CREATE (n:Person{id: $id1})", parameters) );
        session.writeTransaction( tx -> tx.run( "CREATE (n:Person{id: $id2})", parameters) );
        session.writeTransaction( tx -> tx.run( "MATCH\n" +
                                                "  (a:Person),\n" +
                                                "  (b:Person)\n" +
                                                "WHERE a.id = $id1 AND b.id = $id2 \n" +
                                                "CREATE (a)-[r:testType]->(b)\n" +
                                                "RETURN type(r)", parameters) ).consume();
    }
}
