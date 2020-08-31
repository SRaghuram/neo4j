/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedureTest;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static com.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Client.OUTBOUND;
import static com.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Server.INBOUND;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_SNAPPY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
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
            .withSharedCoreParam( CausalClusteringSettings.leader_failure_detection_window, "2s-3s" )
            .withSharedCoreParam( CausalClusteringSettings.election_failure_detection_window, "2s-3s" )
            .withSharedCoreParam( CausalClusteringSettings.compression_implementations, "snappy" )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    private Cluster cluster;
    private CoreClusterMember leader;

    @BeforeAll
    void startUp() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        leader = cluster.awaitLeader();
    }

    @Test
    void shouldSeeOutboundInstalledProtocolsOnLeader()
    {
        String modifiers = new StringJoiner( ",", "[", "]" )
                .add( COMPRESSION_SNAPPY.implementation() )
                .toString();

        ProtocolInfo[] expectedProtocolInfos = cluster.coreMembers()
                .stream()
                .filter( member -> !member.equals( leader ) )
                .map( member -> new ProtocolInfo( OUTBOUND, localhost( member.raftListenAddress() ), RAFT.canonicalName(), "4.0", modifiers ) )
                .toArray( ProtocolInfo[]::new );

        assertEventually( "should see outbound installed protocols on core " + leader.index(),
                () -> installedProtocols( leader.defaultDatabase(), OUTBOUND ),
                new HamcrestCondition<>( hasItems( expectedProtocolInfos ) ),
                60, SECONDS );
    }

    @Test
    void shouldSeeInboundInstalledProtocolsOnLeader()
    {
        assertEventually( "should see inbound installed protocols on core " + leader.index(),
                () -> installedProtocols( leader.defaultDatabase(), INBOUND ),
                new HamcrestCondition<>( hasSize( greaterThanOrEqualTo( cluster.coreMembers().size() - 1 ) ) ),
                60, SECONDS );
    }

    private List<ProtocolInfo> installedProtocols( GraphDatabaseFacade db, String wantedOrientation )
            throws TransactionFailureException, ProcedureException
    {
        List<ProtocolInfo> infos = new LinkedList<>();
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.IMPLICIT, AnonymousContext.read() ) )
        {
            Procedures procedures = tx.procedures();
            int procedureId = procedures.procedureGet( procedureName( "dbms", "cluster", InstalledProtocolsProcedure.PROCEDURE_NAME ) ).id();
            RawIterator<AnyValue[],ProcedureException> itr =
                    procedures.procedureCallRead( procedureId, null,
                            ProcedureCallContext.EMPTY );

            while ( itr.hasNext() )
            {
                AnyValue[] row = itr.next();
                String orientation = ((TextValue) row[0]).stringValue();
                String address = localhost( ((TextValue) row[1]).stringValue() );
                String protocol = ((TextValue) row[2]).stringValue();
                String version = ((TextValue) row[3]).stringValue();
                String modifiers = ((TextValue) row[4]).stringValue();
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
        private final String version;
        private final String modifiers;

        private ProtocolInfo( String orientation, String address, String protocol, String version, String modifiers )
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
            return Objects.equals( orientation, that.orientation ) &&
                   Objects.equals( address, that.address ) &&
                   Objects.equals( protocol, that.protocol ) &&
                   Objects.equals( version, that.version ) &&
                   Objects.equals( modifiers, that.modifiers );
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
