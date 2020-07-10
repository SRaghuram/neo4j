/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.common.StubClusteredDatabaseContext;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.readreplica.CatchupProcessManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.AnyValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.values.storable.BooleanValue.FALSE;
import static org.neo4j.values.storable.BooleanValue.TRUE;
import static org.neo4j.values.storable.Values.utf8Value;

class ReadReplicaToggleProcedureTest
{

    final CatchupProcessManager manager = mock( CatchupProcessManager.class );
    final NamedDatabaseId namedDatabaseId = randomNamedDatabaseId();
    final StubClusteredDatabaseManager databaseService = new StubClusteredDatabaseManager();
    final ReadReplicaToggleProcedure procedure = new ReadReplicaToggleProcedure( databaseService );

    @BeforeEach
    public void setUp()
    {
        Dependencies dependencies = mock( Dependencies.class );
        StubClusteredDatabaseContext dbContext = databaseService.givenDatabaseWithConfig()
                                                                .withDatabaseId( namedDatabaseId )
                                                                .withDependencies( dependencies )
                                                                .register();
        databaseService.registerDatabase( namedDatabaseId, dbContext );
        when( dependencies.resolveDependency( CatchupProcessManager.class ) ).thenReturn( manager );
    }

    @Test
    public void shouldThrowExceptionIfInputIsEmpty()
    {
        //given
        AnyValue[] input = {};

        //when
        var exception = assertThrows( IllegalArgumentException.class, () -> procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) ) );
        assertThat( exception ).hasMessageContaining( "Illegal input:" );
    }

    @Test
    public void shouldThrowExceptionIfInputContainsLessParametersThenExpected()
    {
        //given
        AnyValue[] input = {utf8Value( "test" )};

        //when
        var exception = assertThrows( IllegalArgumentException.class, () -> procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) ) );
        assertThat( exception ).hasMessageContaining( "Input should contains 2 parameters" );
    }

    @Test
    public void shouldThrowExceptionIfParametersHasIllegalType()
    {

        //when string is expected on first position
        var exception1 = assertThrows( IllegalArgumentException.class, () ->
        {
            AnyValue[] input = {TRUE, TRUE};
            procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) );
        } );
        assertThat( exception1 ).hasMessageContaining( "value should have a String representation" );

        //when boolean is expected on second position
        var exception2 = assertThrows( IllegalArgumentException.class, () ->
        {
            AnyValue[] input = {utf8Value( "test" ), utf8Value( "test" )};
            procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) );
        } );

        assertThat( exception2 ).hasMessageContaining( "value should have a Boolean representation" );
    }

    @Test
    public void shouldPauseCatchupProcessIfProcessIsRunning() throws Exception
    {
        AnyValue[] input = {utf8Value( namedDatabaseId.name() ), TRUE};
        when( manager.pauseCatchupProcess() ).thenReturn( true );

        //when
        RawIterator<AnyValue[],ProcedureException> result = procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) );

        //then
        verify( manager ).pauseCatchupProcess();
        assertThat( asList( result ) ).contains( new AnyValue[]{utf8Value( "Catchup process is paused" )} );
    }

    @Test
    public void shouldNotPauseCatchupProcessIfProcessIsPaused() throws Exception
    {
        AnyValue[] input = {utf8Value( namedDatabaseId.name() ), TRUE};
        when( manager.pauseCatchupProcess() ).thenReturn( false );

        //when
        var result = procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) );

        //then
        verify( manager ).pauseCatchupProcess();
        assertThat( asList( result ) ).contains( new AnyValue[]{utf8Value( "Catchup process was already paused" )} );
    }

    @Test
    public void shouldNotPauseCatchupProcessIfProcessIsStoryCopyState() throws Exception
    {
        AnyValue[] input = {utf8Value( namedDatabaseId.name() ), TRUE};
        when( manager.pauseCatchupProcess() ).thenThrow( IllegalStateException.class );

        //when
        var result = procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) );

        //then
        verify( manager ).pauseCatchupProcess();
        assertThat( asList( result ) ).contains( new AnyValue[]{utf8Value( "Catchup process can't be paused" )} );
    }

    @Test
    public void shouldResumeCatchupProcessIfProcessIsPaused() throws Exception
    {
        AnyValue[] input = {utf8Value( namedDatabaseId.name() ), FALSE};
        when( manager.resumeCatchupProcess() ).thenReturn( true );

        //when
        var result = procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) );

        //then
        verify( manager ).resumeCatchupProcess();
        assertThat( asList( result ) ).contains( new AnyValue[]{utf8Value( "Catchup process is resumed" )} );
    }

    @Test
    public void shouldNotResumeCatchupProcessIfProcessIsResumed() throws ProcedureException
    {
        AnyValue[] input = {utf8Value( namedDatabaseId.name() ), FALSE};
        when( manager.resumeCatchupProcess() ).thenReturn( false );

        //when
        var result = procedure.apply( mock( Context.class ), input, mock( ResourceTracker.class ) );

        //then
        verify( manager ).resumeCatchupProcess();
        assertThat( asList( result ) ).contains( new AnyValue[]{utf8Value( "Catchup process was already resumed" )} );
    }
}
