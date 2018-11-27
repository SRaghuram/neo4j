/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import org.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedureTest;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterConfig;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.COMPRESSION_SNAPPY;
import static org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Client.OUTBOUND;
import static org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Server.INBOUND;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * @see InstalledProtocolsProcedureTest
 */
@ClusterExtension
public class InstalledProtocolsProcedureIT
{
    @Inject
    private ClusterFactory clusterFactory;

    public final ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "2s" )
            .withSharedCoreParam( CausalClusteringSettings.compression_implementations, "snappy" )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    private Cluster<?> cluster;
    private CoreClusterMember leader;

    @BeforeAll
    void startUp() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        leader = cluster.awaitLeader();
    }

    @Test
    void shouldSeeOutboundInstalledProtocolsOnLeader() throws Throwable
    {
        String modifiers = new StringJoiner( ",", "[", "]" )
                .add( COMPRESSION_SNAPPY.implementation() )
                .toString();

        ProtocolInfo[] expectedProtocolInfos = cluster.coreMembers()
                .stream()
                .filter( member -> !member.equals( leader ) )
                .map( member -> new ProtocolInfo( OUTBOUND, localhost( member.raftListenAddress() ), RAFT.canonicalName(), 2, modifiers ) )
                .toArray( ProtocolInfo[]::new );

        assertEventually( "should see outbound installed protocols on core " + leader.serverId(),
                () -> installedProtocols( leader.database(), OUTBOUND ),
                hasItems( expectedProtocolInfos ),
                60, SECONDS );
    }

    @Test
    void shouldSeeInboundInstalledProtocolsOnLeader() throws Throwable
    {
        assertEventually( "should see inbound installed protocols on core " + leader.serverId(),
                () -> installedProtocols( leader.database(), INBOUND ),
                hasSize( greaterThanOrEqualTo( cluster.coreMembers().size() - 1 ) ),
                60, SECONDS );
    }

    private List<ProtocolInfo> installedProtocols( GraphDatabaseFacade db, String wantedOrientation )
            throws TransactionFailureException, ProcedureException
    {
        List<ProtocolInfo> infos = new LinkedList<>();
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( Transaction tx = kernel.beginTransaction( Transaction.Type.implicit, AnonymousContext.read() ) )
        {
            RawIterator<Object[],ProcedureException> itr =
                    tx.procedures().procedureCallRead( procedureName( "dbms", "cluster", InstalledProtocolsProcedure.PROCEDURE_NAME ), null );

            while ( itr.hasNext() )
            {
                Object[] row = itr.next();
                String orientation = (String) row[0];
                String address = localhost( (String) row[1] );
                String protocol = (String) row[2];
                long version = (long) row[3];
                String modifiers = (String) row[4];
                if ( orientation.equals( wantedOrientation ) )
                {
                    infos.add( new ProtocolInfo( orientation, address, protocol, version, modifiers ) );
                }
            }
            return infos;
        }
    }

    private String localhost( String uri )
    {
        return uri.replace( "127.0.0.1", "localhost" );
    }

    private static class ProtocolInfo
    {
        private final String orientation;
        private final String address;
        private final String protocol;
        private final long version;
        private final String modifiers;

        private ProtocolInfo( String orientation, String address, String protocol, long version, String modifiers )
        {
            this.orientation = orientation;
            this.address = address;
            this.protocol = protocol;
            this.version = version;
            this.modifiers = modifiers;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ProtocolInfo that = (ProtocolInfo) o;
            return version == that.version && Objects.equals( orientation, that.orientation ) && Objects.equals( address, that.address ) &&
                    Objects.equals( protocol, that.protocol ) && Objects.equals( modifiers, that.modifiers );
        }

        @Override
        public int hashCode()
        {

            return Objects.hash( orientation, address, protocol, version, modifiers );
        }

        @Override
        public String toString()
        {
            return "ProtocolInfo{" + "orientation='" + orientation + '\'' + ", address='" + address + '\'' + ", protocol='" + protocol + '\'' + ", version=" +
                    version + ", modifiers='" + modifiers + '\'' + '}';
        }
    }
}
