/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.cluster.InstanceId;
import org.neo4j.function.Suppliers;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdatePullingTransactionObligationFulfillerTest
{
    private final UpdatePuller updatePuller = mock( UpdatePuller.class );
    private final HighAvailabilityMemberStateMachine machine = mock( HighAvailabilityMemberStateMachine.class );
    private final InstanceId serverId = new InstanceId( 42 );

    @Before
    public void setup() throws Throwable
    {
        doAnswer( invocation -> ((UpdatePuller.Condition) invocation.getArgument( 0 )).evaluate( 33, 34 )
        ).when( updatePuller ).pullUpdates( any( UpdatePuller.Condition.class ), anyBoolean() );
    }

    @Test
    public void shouldNotThrowNPEWhenAskedToFulFilledButNotYetHavingARoleAssigned() throws Throwable
    {
        // Given
        UpdatePullingTransactionObligationFulfiller fulfiller =
                new UpdatePullingTransactionObligationFulfiller( updatePuller, machine, serverId,
                        Suppliers.singleton( mock( TransactionIdStore.class ) ) );

        // When
        fulfiller.fulfill( 1 );

        // Then
        // it doesn't blow up
    }

    @Test
    public void shouldUpdateTransactionIdStoreCorrectly() throws Throwable
    {
        // Given
        TransactionIdStore store1 = mock( TransactionIdStore.class );
        TransactionIdStore store2 = mock( TransactionIdStore.class );
        @SuppressWarnings( "unchecked" )
        Supplier<TransactionIdStore> supplier = mock( Supplier.class );
        when( supplier.get() ).thenReturn( store1, store2 );

        doAnswer( invocation ->
        {
            ((HighAvailabilityMemberListener) invocation.getArgument( 0 )).slaveIsAvailable(
                    new HighAvailabilityMemberChangeEvent( null, null, serverId, null )
            );
            return null;
        } ).when( machine ).addHighAvailabilityMemberListener( any( HighAvailabilityMemberListener.class ) );

        doAnswer( invocation ->
        {
            ((HighAvailabilityMemberListener) invocation.getArgument( 0 )).instanceStops(
                    new HighAvailabilityMemberChangeEvent( null, null, serverId, null )
            );
            return null;
        } ).when( machine ).removeHighAvailabilityMemberListener( any( HighAvailabilityMemberListener.class ) );

        UpdatePullingTransactionObligationFulfiller fulfiller =
                new UpdatePullingTransactionObligationFulfiller( updatePuller, machine, serverId, supplier );

        // When
        fulfiller.start();
        fulfiller.fulfill( 1 );
        fulfiller.stop();
        fulfiller.fulfill( 2 );
        fulfiller.start();
        fulfiller.fulfill( 3 );
        fulfiller.stop();
        fulfiller.fulfill( 4 );

        // Then
        verify( store1, times( 1 ) ).getLastClosedTransactionId();
        verify( store2, times( 1 ) ).getLastClosedTransactionId();
    }
}
