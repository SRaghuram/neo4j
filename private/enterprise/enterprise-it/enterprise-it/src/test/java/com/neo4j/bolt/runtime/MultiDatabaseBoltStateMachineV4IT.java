/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.bolt.v4.runtime.AutoCommitState;
import org.neo4j.bolt.v4.runtime.FailedState;
import org.neo4j.bolt.v4.runtime.InTransactionState;
import org.neo4j.bolt.v4.runtime.ReadyState;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.runtime.statemachine.StatementProcessor.EMPTY;
import static org.neo4j.bolt.testing.BoltConditions.containsRecord;
import static org.neo4j.bolt.testing.BoltConditions.failedWithStatus;
import static org.neo4j.bolt.testing.BoltConditions.succeeded;
import static org.neo4j.bolt.testing.BoltConditions.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v4.messaging.AbstractStreamingMessage.STREAM_LIMIT_UNLIMITED;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.DB_NAME_KEY;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.map;

class MultiDatabaseBoltStateMachineV4IT extends MultiDatabaseBoltStateMachineTestBase
{
    @Test
    void shouldAllowSessionRunOnDifferentDatabase() throws Throwable
    {
        // Given
        DatabaseManagementService managementService = managementService();
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );
        var machine = newStateMachineInReadyState();

        // When
        RecordedBoltResponse first = sessionRun( "Unwind [1, 2, 3] as n return n", machine, "first" );
        assertThat( first ).satisfies( containsRecord( 1L ) );
        reset( machine );

        // Then
        RecordedBoltResponse second = sessionRun( "Unwind [4, 5] as n return n", machine, "second" );
        assertThat( second ).satisfies( containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldAllowTransactionRunOnDifferentDatabase() throws Throwable
    {
        // Given
        DatabaseManagementService managementService = managementService();
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );
        var machine = newStateMachineInReadyState();

        // When
        RecordedBoltResponse first = txRun( "Unwind [1, 2, 3] as n return n", machine, "first" );
        assertThat( first ).satisfies( containsRecord( 1L ) );
        reset( machine );

        // Then
        RecordedBoltResponse second = txRun( "Unwind [4, 5] as n return n", machine, "second" );
        assertThat( second ).satisfies( containsRecord( 4L ) );
        reset( machine );
    }

    @Override
    protected void reset( AbstractBoltStateMachine machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.interrupt();
        machine.process( ResetMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response ).satisfies( succeeded() );
        assertThat( machine.state() ).isInstanceOf( ReadyState.class );
        assertThat( machine.connectionState().getStatementProcessor() ).isEqualTo( EMPTY );
    }

    @Override
    protected RecordedBoltResponse sessionRun( String query, AbstractBoltStateMachine machine ) throws Throwable
    {
        return sessionRun( query, machine, ABSENT_DB_NAME );
    }

    @Override
    void sessionBeginTx( AbstractBoltStateMachine machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // BEGIN
        machine.process( new BeginMessage(), recorder );
        assertThat( recorder.nextResponse() ).satisfies( succeeded() );
        assertThat( machine.state() ).isInstanceOf( InTransactionState.class );
        verifyStatementProcessorNotEmpty( machine );
    }

    @Override
    protected RecordedBoltResponse txRun( String query, AbstractBoltStateMachine machine ) throws Throwable
    {
        return txRun( query, machine, ABSENT_DB_NAME );
    }

    @Override
    protected void runWithFailure( String query, AbstractBoltStateMachine machine, Status status, boolean isEmpty ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query ), recorder );
        assertThat( recorder.nextResponse() ).satisfies( failedWithStatus( status ) );
        assertThat( machine.state() ).isInstanceOf( FailedState.class );
        verifyStatementProcessor( machine, isEmpty );

        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response ).satisfies( wasIgnored() );
        assertThat( machine.state() ).isInstanceOf( FailedState.class );
        verifyStatementProcessor( machine, isEmpty );
    }

    @Override
    protected AbstractBoltStateMachine newStateMachineInReadyState() throws Throwable
    {
        BoltStateMachineV4 machine = (BoltStateMachineV4) env.newMachine( BoltProtocolV4.VERSION, BOLT_CHANNEL );
        machine.process( newHelloMessage(), nullResponseHandler() );
        return machine;
    }

    private RecordedBoltResponse sessionRun( String query, AbstractBoltStateMachine machine, String databaseName ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query, EMPTY_PARAMS, EMPTY_PARAMS, List.of(), null, AccessMode.WRITE, Map.of(), databaseName ), recorder );
        assertThat( recorder.nextResponse() ).satisfies( succeeded() );
        assertThat( machine.state() ).isInstanceOf( AutoCommitState.class );
        verifyStatementProcessorNotEmpty( machine, databaseName );

        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response ).satisfies( succeeded() );
        assertThat( machine.state() ).isInstanceOf( ReadyState.class );
        verifyStatementProcessorIsEmpty( machine );
        return response;
    }

    private RecordedBoltResponse txRun( String query, AbstractBoltStateMachine machine, String databaseName ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // BEGIN
        var beginMetadata = map( new String[]{DB_NAME_KEY}, new AnyValue[]{stringValue( databaseName )} );
        machine.process( new BeginMessage( beginMetadata, List.of(), null, AccessMode.WRITE, Map.of(), databaseName ), recorder );
        assertThat( recorder.nextResponse() ).satisfies( succeeded() );
        assertThat( machine.state() ).isInstanceOf( InTransactionState.class );
        verifyStatementProcessorNotEmpty( machine, databaseName );

        // RUN
        machine.process( new RunMessage( query, EMPTY_PARAMS ), recorder );
        assertThat( recorder.nextResponse() ).satisfies( succeeded() );
        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response ).satisfies( succeeded() );

        assertThat( machine.state() ).isInstanceOf( InTransactionState.class );
        verifyStatementProcessorNotEmpty( machine, databaseName );

        // COMMIT
        machine.process( COMMIT_MESSAGE, recorder );
        assertThat( recorder.nextResponse() ).satisfies( succeeded() );
        assertThat( machine.state() ).isInstanceOf( ReadyState.class );
        verifyStatementProcessorIsEmpty( machine );
        return response;
    }

    /**
     * Verify the database is set in the current connection context
     */
    private static void verifyStatementProcessorNotEmpty( AbstractBoltStateMachine machine, String databaseName )
    {
        StatementProcessor processor = machine.connectionState().getStatementProcessor();
        if ( !Objects.equals( databaseName, ABSENT_DB_NAME ) )
        {
            assertThat( processor.databaseName() ).isEqualTo( databaseName );
        }
        else
        {
            assertThat( processor ).isNotEqualTo( EMPTY );
        }
    }

    private static PullMessage newPullAll() throws BoltIOException
    {
        return new PullMessage( ValueUtils.asMapValue( Collections.singletonMap( "n", STREAM_LIMIT_UNLIMITED ) ) );
    }
}
