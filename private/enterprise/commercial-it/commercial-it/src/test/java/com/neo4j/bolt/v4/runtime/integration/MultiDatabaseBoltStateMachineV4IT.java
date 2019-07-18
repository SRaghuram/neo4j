/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.v4.runtime.integration;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.runtime.BoltStateMachineV1;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.runtime.StatementProcessor.EMPTY;
import static org.neo4j.bolt.testing.BoltMatchers.containsRecord;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v4.messaging.AbstractStreamingMessage.STREAM_LIMIT_UNLIMITED;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

class MultiDatabaseBoltStateMachineV4IT extends MultiDatabaseBoltStateMachineTestBase
{
    @Test
    void shouldAllowSessionRunOnDifferentDatabase() throws Throwable
    {
        // Given
        DatabaseManagementService managementService = managementService();
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );
        BoltStateMachineV1 machine = newStateMachineInReadyState();

        // When
        RecordedBoltResponse first = sessionRun( "Unwind [1, 2, 3] as n return n", machine, "first" );
        assertThat( first, containsRecord( 1L ) );
        reset( machine );

        // Then
        RecordedBoltResponse second = sessionRun( "Unwind [4, 5] as n return n", machine, "second" );
        assertThat( second, containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldAllowTransactionRunOnDifferentDatabase() throws Throwable
    {
        // Given
        DatabaseManagementService managementService = managementService();
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );
        BoltStateMachineV1 machine = newStateMachineInReadyState();

        // When
        RecordedBoltResponse first = txRun( "Unwind [1, 2, 3] as n return n", machine, "first" );
        assertThat( first, containsRecord( 1L ) );
        reset( machine );

        // Then
        RecordedBoltResponse second = txRun( "Unwind [4, 5] as n return n", machine, "second" );
        assertThat( second, containsRecord( 4L ) );
        reset( machine );
    }

    @Override
    protected void reset( BoltStateMachineV1 machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.interrupt();
        machine.process( ResetMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        assertThat( machine.connectionState().getStatementProcessor(), equalTo( EMPTY ) );
    }

    @Override
    protected RecordedBoltResponse sessionRun( String query, BoltStateMachineV1 machine ) throws Throwable
    {
        return sessionRun( query, machine, ABSENT_DB_NAME );
    }

    @Override
    void sessionBeginTx( BoltStateMachineV1 machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // BEGIN
        machine.process( new BeginMessage(), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
        verifyStatementProcessorNotEmpty( machine );
    }

    @Override
    protected RecordedBoltResponse txRun( String query, BoltStateMachineV1 machine ) throws Throwable
    {
        return txRun( query, machine, ABSENT_DB_NAME );
    }

    @Override
    protected void runWithFailure( String query, BoltStateMachineV1 machine, Status status, boolean isEmpty ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query ), recorder );
        assertThat( recorder.nextResponse(), failedWithStatus( status ) );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
        verifyStatementProcessor( machine, isEmpty );

        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, wasIgnored() );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
        verifyStatementProcessor( machine, isEmpty );
    }

    @Override
    protected BoltStateMachineV1 newStateMachineInReadyState() throws Throwable
    {
        BoltStateMachineV4 machine = (BoltStateMachineV4) env.newMachine( BoltProtocolV4.VERSION, BOLT_CHANNEL );
        machine.process( newHelloMessage(), nullResponseHandler() );
        return machine;
    }

    private RecordedBoltResponse sessionRun( String query, BoltStateMachineV1 machine, String databaseName ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query, EMPTY_PARAMS, EMPTY_PARAMS, List.of(), null, AccessMode.WRITE, Map.of(), databaseName ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( AutoCommitState.class ) );
        verifyStatementProcessorNotEmpty( machine, databaseName );

        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorIsEmpty( machine );
        return response;
    }

    private RecordedBoltResponse txRun( String query, BoltStateMachineV1 machine, String databaseName ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // BEGIN
        machine.process( new BeginMessage( EMPTY_PARAMS, List.of(), null, AccessMode.WRITE, Map.of(), databaseName ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
        verifyStatementProcessorNotEmpty( machine, databaseName );

        // RUN
        machine.process( new RunMessage( query, EMPTY_PARAMS ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );

        assertThat( machine.state(), instanceOf( InTransactionState.class ) );
        verifyStatementProcessorNotEmpty( machine, databaseName );

        // COMMIT
        machine.process( COMMIT_MESSAGE, recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorIsEmpty( machine );
        return response;
    }

    /**
     * Verify the database is set in the current connection context
     */
    private static void verifyStatementProcessorNotEmpty( BoltStateMachineV1 machine, String databaseName )
    {
        StatementProcessor processor = machine.connectionState().getStatementProcessor();
        if ( !Objects.equals( databaseName, ABSENT_DB_NAME ) )
        {
            assertThat( processor.databaseName(), equalTo( databaseName ) );
        }
        else
        {
            assertThat( processor, not( EMPTY ) );
        }
    }

    private static PullMessage newPullAll() throws BoltIOException
    {
        return new PullMessage( ValueUtils.asMapValue( Collections.singletonMap( "n", STREAM_LIMIT_UNLIMITED ) ) );
    }
}
