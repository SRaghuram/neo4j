/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.MockCatchupClient;
import com.neo4j.causalclustering.catchup.v4.info.InfoProvider;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.utf8Value;

class WaitProcedureTest
{
    private final LogProvider logProvider = NullLogProvider.nullLogProvider();
    private final TextValue targetDatabaseId = utf8Value( randomUUID().toString() );
    private final TextValue targetDatabaseName = utf8Value( "foo" );
    private final FakeClock fakeClock = Clocks.fakeClock();
    private final CoreServerIdentity identity = new InMemoryCoreServerIdentity();
    private final ServerId myself = identity.serverId();
    private final ServerId other1 = new ServerId( randomUUID() );
    private final ServerId other2 = new ServerId( randomUUID() );
    private final ServerId replica1 = new ServerId( randomUUID() );
    private final ServerId replica2 = new ServerId( randomUUID() );

    @Test
    void signatureIsCorrect()
    {

        var topologyService = mock( CoreTopologyService.class );
        var procedure =
                WaitProcedure.clustered( topologyService, identity, fakeClock,
                        catchupClientFactory( InfoResponse.create( 0, null ) ), logProvider,
                        new InfoProvider( null, null ) );

        var signature = procedure.signature();

        assertEquals( "dbms.admin.wait", signature.name().toString() );
        assertEquals( List.of( inputField( "systemTxId", Neo4jTypes.NTNumber ),
                inputField( "databaseId", Neo4jTypes.NTString ),
                inputField( "databaseName", Neo4jTypes.NTString ),
                inputField( "timeoutSeconds", Neo4jTypes.NTNumber ) ),
                signature.inputSignature() );
        assertTrue( signature.systemProcedure() );
        assertTrue( signature.internal() );
        assertEquals(
                List.of( outputField( "address", Neo4jTypes.NTString ),
                        outputField( "state", Neo4jTypes.NTString ),
                        outputField( "message", Neo4jTypes.NTString ),
                        outputField( "success", Neo4jTypes.NTBoolean ) ),
                signature.outputSignature() );
    }

    @Test
    void toFewInputParams()
    {
        var procedure = WaitProcedure.clustered( null, identity, null, null, logProvider, null );
        var exception =
                assertThrows( IllegalArgumentException.class,
                        () -> procedure.apply( null, new AnyValue[]{Values.intValue( 1 ), Values.stringValue( "id" )}, null ) );
        assertThat( exception.getMessage() ).startsWith( "Expected exactly 4 input parameters with input signature" );
    }

    @Test
    void remoteCallsShouldBeIncompleteIfReconciliatedIdIsNotMet() throws Exception
    {
        // given
        final var topologyService = createTopologyService();

        var reconciledTransactionTracker = mock( ReconciledTransactionTracker.class );
        var desiredReconcilatedId = 1L;
        var remoteReconcilatedId = 0;
        when( reconciledTransactionTracker.getLastReconciledTransactionId() ).thenReturn( desiredReconcilatedId );

        var provider = mockReconciliationProvider( 1L, null );

        var procedure = WaitProcedure.clustered( topologyService, identity, Clocks.systemClock(),
                catchupClientFactory( InfoResponse.create( remoteReconcilatedId, null ) ), logProvider, provider );

        // when
        var desiredTxId = Values.longValue( 1 );
        var timeout = Values.longValue( 0 );
        final var servers =
                procedure.apply( null, new AnyValue[]{desiredTxId, targetDatabaseId, targetDatabaseName, timeout}, null );

        List<AnyValue[]> results = new ArrayList<>();
        while ( servers.hasNext() )
        {
            results.add( servers.next() );
        }

        assertThat( results ).containsExactlyInAnyOrder(
                arrayOf( getBoltAddress( topologyService, myself ).toString(), "CaughtUp", "caught up", false ),
                arrayOf( getBoltAddress( topologyService, other1 ).toString(), "Incomplete", "server is still catching up", false ),
                arrayOf( getBoltAddress( topologyService, other2 ).toString(), "Incomplete", "server is still catching up", false ),
                arrayOf( getBoltAddress( topologyService, replica1 ).toString(), "Incomplete", "server is still catching up", false ),
                arrayOf( getBoltAddress( topologyService, replica2 ).toString(), "Incomplete", "server is still catching up", false ) );
    }

    private static SocketAddress getBoltAddress( CoreTopologyService topologyService, ServerId serverId )
    {
        var coreServerInfo = topologyService.allCoreServers().get( serverId );
        if ( coreServerInfo != null )
        {
            return coreServerInfo.boltAddress();
        }
        return topologyService.allReadReplicas().get( serverId ).boltAddress();
    }

    @Test
    void shouldSuccessfullyGetCaughtUpFromRemoteServers() throws ProcedureException, UnavailableException
    {
        // given
        final var topologyService = createTopologyService();

        var reconciledTransactionTracker = mock( ReconciledTransactionTracker.class );
        when( reconciledTransactionTracker.getLastReconciledTransactionId() ).thenReturn( 1L );

        var procedure = WaitProcedure.clustered( topologyService, identity, Clocks.systemClock(),
                catchupClientFactory( InfoResponse.create( 1, null ) ), logProvider, mockReconciliationProvider( 1L, null ) );

        // when
        var desiredTxId = Values.longValue( 1 );
        var timeout = Values.longValue( 0 );
        final var servers = procedure.apply( null, new AnyValue[]{desiredTxId, targetDatabaseId, targetDatabaseName, timeout}, null );

        List<AnyValue[]> results = new ArrayList<>();
        while ( servers.hasNext() )
        {
            results.add( servers.next() );
        }

        assertThat( results ).containsExactlyInAnyOrder(
                arrayOf( getBoltAddress( topologyService, myself ).toString(), "CaughtUp", "caught up", true ),
                arrayOf( getBoltAddress( topologyService, other1 ).toString(), "CaughtUp", "caught up", true ),
                arrayOf( getBoltAddress( topologyService, other2 ).toString(), "CaughtUp", "caught up", true ),
                arrayOf( getBoltAddress( topologyService, replica1 ).toString(), "CaughtUp", "caught up", true ),
                arrayOf( getBoltAddress( topologyService, replica2 ).toString(), "CaughtUp", "caught up", true ) );
    }

    @Test
    void shouldGiveFailedStatusesIfRequestFails() throws ProcedureException, UnavailableException
    {
        // given
        final var topologyService = createTopologyService();

        var reconciledTransactionTracker = mock( ReconciledTransactionTracker.class );
        when( reconciledTransactionTracker.getLastReconciledTransactionId() ).thenReturn( 1L );

        var procedure = WaitProcedure.clustered( topologyService, identity, Clocks.systemClock(),
                catchupClientFactory( InfoResponse.create( -1, "Error" ) ), logProvider, mockReconciliationProvider( -1, "Error" ) );

        // when
        var desiredTxId = Values.longValue( 1 );
        var timeout = Values.longValue( 0 );
        final var servers = procedure.apply( null, new AnyValue[]{desiredTxId, targetDatabaseId, targetDatabaseName, timeout}, null );

        List<AnyValue[]> results = new ArrayList<>();
        while ( servers.hasNext() )
        {
            results.add( servers.next() );
        }

        var execptedError =
                "Not caught up and has failure for " +
                DatabaseIdFactory.from( targetDatabaseName.stringValue(), UUID.fromString( targetDatabaseId.stringValue() ) ) + " Failure: Error";
        assertThat( results ).containsExactlyInAnyOrder(
                arrayOf( getBoltAddress( topologyService, myself ).toString(), "Failed",
                        execptedError, false ),
                arrayOf( getBoltAddress( topologyService, other1 ).toString(), "Failed",
                        execptedError, false ),
                arrayOf( getBoltAddress( topologyService, other2 ).toString(), "Failed",
                        execptedError, false ),
                arrayOf( getBoltAddress( topologyService, replica1 ).toString(), "Failed",
                        execptedError, false ),
                arrayOf( getBoltAddress( topologyService, replica2 ).toString(), "Failed",
                        execptedError, false ) );
    }

    @Test
    void shouldGiveFailedStatusesIfRequestFailsAndIsUpToDate() throws ProcedureException, UnavailableException
    {
        // given
        final var topologyService = createTopologyService();

        var reconciledTransactionTracker = mock( ReconciledTransactionTracker.class );
        when( reconciledTransactionTracker.getLastReconciledTransactionId() ).thenReturn( 1L );

        var procedure = WaitProcedure.clustered( topologyService, identity, Clocks.systemClock(),
                catchupClientFactory( InfoResponse.create( 1, "Error" ) ), logProvider, mockReconciliationProvider( 1, "Error" ) );

        // when
        var desiredTxId = Values.longValue( 1 );
        var timeout = Values.longValue( 0 );
        final var servers = procedure.apply( null, new AnyValue[]{desiredTxId, targetDatabaseId, targetDatabaseName, timeout}, null );

        List<AnyValue[]> results = new ArrayList<>();
        while ( servers.hasNext() )
        {
            results.add( servers.next() );
        }

        var exepctedError = "Caught up but has failure for " +
                            DatabaseIdFactory.from( targetDatabaseName.stringValue(), UUID.fromString( targetDatabaseId.stringValue() ) ) + " Failure: Error";
        assertThat( results ).containsExactlyInAnyOrder(
                arrayOf( getBoltAddress( topologyService, myself ).toString(), "Failed", exepctedError, false ),
                arrayOf( getBoltAddress( topologyService, other1 ).toString(), "Failed", exepctedError, false ),
                arrayOf( getBoltAddress( topologyService, other2 ).toString(), "Failed", exepctedError, false ),
                arrayOf( getBoltAddress( topologyService, replica1 ).toString(), "Failed", exepctedError, false ),
                arrayOf( getBoltAddress( topologyService, replica2 ).toString(), "Failed", exepctedError, false ) );
    }

    @Test
    void shouldAlsoWorkForStandalone() throws UnavailableException, ProcedureException
    {
        var boltAddress = addressesForCore( 0 ).boltAddress();
        var standalone = WaitProcedure.standalone( myself, boltAddress, Clocks.systemClock(), logProvider,
                mockReconciliationProvider( 1, null ) );

        var desiredTxId = Values.longValue( 1 );
        var timeout = Values.longValue( 0 );
        var result = standalone.apply( null, new AnyValue[]{desiredTxId, targetDatabaseId, targetDatabaseName, timeout}, null );

        assertTrue( result.hasNext() );
        var anyValues = result.next();
        assertFalse( result.hasNext() );
        assertThat( anyValues ).containsExactly( arrayOf( boltAddress.toString(), "CaughtUp", "caught up", true ) );
    }

    private InfoProvider mockReconciliationProvider( long reconciledTxId, String reconcilationError ) throws UnavailableException
    {
        var reconcilationInfoProvider = mock( InfoProvider.class );
        when( reconcilationInfoProvider.getInfo( any() ) ).thenReturn( InfoResponse.create( reconciledTxId, reconcilationError ) );
        return reconcilationInfoProvider;
    }

    CatchupClientFactory catchupClientFactory( InfoResponse response )
    {
        var mockCatchupClient = new MockCatchupClient( ApplicationProtocols.CATCHUP_4_0,
                new MockCatchupClient.MockClientV4( MockCatchupClient.responses().withReconciledTxId( response ),
                        new TestDatabaseIdRepository() ) );
        var catchupClientFactory = mock( CatchupClientFactory.class );
        when( catchupClientFactory.getClient( any(), any() ) ).thenReturn( mockCatchupClient );
        return catchupClientFactory;
    }

    private AnyValue[] arrayOf( String serverId, String state, String message, boolean success )
    {
        return new AnyValue[]{utf8Value( serverId ), utf8Value( state ), utf8Value( message ), booleanValue( success )};
    }

    private CoreTopologyService createTopologyService()
    {
        final var topologyService = mock( CoreTopologyService.class );

        Map<ServerId,CoreServerInfo> coreMembers = new HashMap<>();

        coreMembers.put( myself, addressesForCore( 0, Set.of() ) );
        coreMembers.put( other1, addressesForCore( 1, Set.of() ) );
        coreMembers.put( other2, addressesForCore( 2, Set.of() ) );

        Map<ServerId,ReadReplicaInfo> replicaMembers = new HashMap<>();

        replicaMembers.put( replica1, addressesForReadReplica( 4, Set.of() ) );
        replicaMembers.put( replica2, addressesForReadReplica( 5, Set.of() ) );

        when( topologyService.allCoreServers() ).thenReturn( coreMembers );
        when( topologyService.allReadReplicas() ).thenReturn( replicaMembers );
        return topologyService;
    }
}
