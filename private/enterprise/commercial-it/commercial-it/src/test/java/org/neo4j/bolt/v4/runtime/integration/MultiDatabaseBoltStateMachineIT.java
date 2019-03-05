/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt.v4.runtime.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Objects;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.PullNMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.bolt.v4.runtime.AutoCommitState;
import org.neo4j.bolt.v4.runtime.FailedState;
import org.neo4j.bolt.v4.runtime.InTransactionState;
import org.neo4j.bolt.v4.runtime.ReadyState;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

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
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;

class MultiDatabaseBoltStateMachineIT
{
    private static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    private static final String USER_AGENT = "BoltConnectionIT/0.0";
    private static final BoltChannel BOLT_CHANNEL = BoltTestUtil.newTestBoltChannel( "conn-v4-test-boltchannel-id" );

    @RegisterExtension
    static final SessionExtension env = new SessionExtension();

    private DatabaseManager databaseManager()
    {
        return env.databaseManager();
    }

    @Test
    void shouldAllowSessionRunOnDefaultDatabase() throws Throwable
    {
        BoltStateMachineV4 machine = newStateMachineInReadyState();

        RecordedBoltResponse response = sessionRun( "Unwind [1, 2, 3] as n return n", machine );
        assertThat( response, containsRecord( 1L ) );
        reset( machine );
    }

    @Test
    void shouldAllowTxRunOnDefaultDatabase() throws Throwable
    {
        BoltStateMachineV4 machine = newStateMachineInReadyState();

        RecordedBoltResponse response = txRun( "Unwind [1, 2, 3] as n return n", machine );
        assertThat( response, containsRecord( 1L ) );
        reset( machine );
    }

    @Test
    void shouldAllowSessionRunOnDifferentDatabase() throws Throwable
    {
        // Given
        DatabaseManager databaseManager = databaseManager();
        databaseManager.createDatabase( "first" );
        databaseManager.createDatabase( "second" );
        BoltStateMachineV4 machine = newStateMachineInReadyState();

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
        DatabaseManager databaseManager = databaseManager();
        databaseManager.createDatabase( "first" );
        databaseManager.createDatabase( "second" );
        BoltStateMachineV4 machine = newStateMachineInReadyState();

        // When
        RecordedBoltResponse first = txRun( "Unwind [1, 2, 3] as n return n", machine, "first" );
        assertThat( first, containsRecord( 1L ) );
        reset( machine );

        // Then
        RecordedBoltResponse second = txRun( "Unwind [4, 5] as n return n", machine, "second" );
        assertThat( second, containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldAllowAnotherSessionRunAfterFailure() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = newStateMachineInReadyState();

        // When
        sessionRunWithFailure( "Invalid", machine, SyntaxError );
        reset( machine );

        // Then
        RecordedBoltResponse second = sessionRun( "Unwind [4, 5] as n return n", machine );
        assertThat( second, containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldAllowAnotherTxRunAfterFailure() throws Throwable
    {
        // Given
        BoltStateMachineV4 machine = newStateMachineInReadyState();

        // When
        sessionRunWithFailure( "Invalid", machine, SyntaxError );
        reset( machine );

        // Then
        RecordedBoltResponse second = txRun( "Unwind [4, 5] as n return n", machine );
        assertThat( second, containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldErrorIfDatabaseNotExists() throws Throwable
    {
        BoltStateMachineV4 machine = newStateMachineInReadyState();
        sessionRunWithFailure( "RETURN 1", machine, "not_exists", Status.Request.Invalid, true );
    }

    @Test
    void shouldErrorIfDatabaseStopped() throws Throwable
    {
        DatabaseManager databaseManager = databaseManager();
        databaseManager.createDatabase( "stopped" );
        databaseManager.stopDatabase( "stopped" );

        BoltStateMachineV4 machine = newStateMachineInReadyState();
        sessionRunWithFailure( "RETURN 1", machine, "stopped", DatabaseUnavailable );
    }

    /**
     * Simulate a driver session close
     */
    private void reset( BoltStateMachineV4 machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.interrupt();
        machine.process( ResetMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        assertThat( machine.connectionState().getStatementProcessor(), equalTo( EMPTY ) );
    }

    /**
     * Simulates a driver session run using BoltV4
     */
    private RecordedBoltResponse sessionRun( String query, BoltStateMachineV4 machine, String databaseName ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query, EMPTY_PARAMS, asMapValue( map( "db_name", databaseName ) ) ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( AutoCommitState.class ) );
        verifyStatementProcessorNotEmpty( machine, databaseName );

        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorNotEmpty( machine, databaseName );
        return response;
    }

    private RecordedBoltResponse sessionRun( String query, BoltStateMachineV4 machine ) throws Throwable
    {
        return sessionRun( query, machine, ABSENT_DB_NAME );
    }

    /**
     * Simulate a session run failed with an error
     */
    private void sessionRunWithFailure( String query, BoltStateMachineV4 machine, String databaseName, Status status, boolean isStatementProcessorEmpty )
            throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query, EMPTY_PARAMS, asMapValue( map( "db_name", databaseName ) ) ), recorder );
        assertThat( recorder.nextResponse(), failedWithStatus( status ) );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
        verifyStatementProcessor( machine, databaseName, isStatementProcessorEmpty );

        // PULL_ALL
        machine.process( newPullAll(), recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, wasIgnored() );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
        verifyStatementProcessor( machine, databaseName, isStatementProcessorEmpty );
    }

    private void sessionRunWithFailure( String query, BoltStateMachineV4 machine, String databaseName, Status status ) throws Throwable
    {
        sessionRunWithFailure( query, machine, databaseName, status, false );
    }

    private void sessionRunWithFailure( String query, BoltStateMachineV4 machine, Status status ) throws Throwable
    {
        sessionRunWithFailure( query, machine, ABSENT_DB_NAME, status, false );
    }

    /**
     * Simulates a driver tx run using BoltV4
     */
    private RecordedBoltResponse txRun( String query, BoltStateMachineV4 machine, String databaseName ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // BEGIN
        machine.process( new BeginMessage( asMapValue( map( "db_name", databaseName ) ) ), recorder );
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
        verifyStatementProcessorNotEmpty( machine, databaseName );
        return response;
    }

    private RecordedBoltResponse txRun( String query, BoltStateMachineV4 machine ) throws Throwable
    {
        return txRun( query, machine, ABSENT_DB_NAME );
    }

    private void verifyStatementProcessor( BoltStateMachineV4 machine, String databaseName, boolean isStatementProcessorEmpty )
    {
        if ( isStatementProcessorEmpty )
        {
            assertThat( machine.connectionState().getStatementProcessor(), equalTo( EMPTY ) );
        }
        else
        {
            verifyStatementProcessorNotEmpty( machine, databaseName );
        }
    }

    /**
     * Verify the database is set in the current connection context
     */
    private void verifyStatementProcessorNotEmpty( BoltStateMachineV4 machine, String databaseName )
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

    private BoltStateMachineV4 newStateMachineInReadyState() throws Throwable
    {
        BoltStateMachineV4 machine = (BoltStateMachineV4) env.newMachine( BoltProtocolV4.VERSION, BOLT_CHANNEL );
        machine.process( newHelloMessage(), nullResponseHandler() );
        return machine;
    }

    private static HelloMessage newHelloMessage()
    {
        return new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) );
    }

    private static PullNMessage newPullAll() throws BoltIOException
    {
        return new PullNMessage( ValueUtils.asMapValue( Collections.singletonMap( "n", Long.MAX_VALUE ) ) );
    }
}
