/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.runtime;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.SessionExtension;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.neo4j.bolt.runtime.statemachine.StatementProcessor.EMPTY;
import static org.neo4j.bolt.testing.BoltConditions.containsRecord;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

@Execution( SAME_THREAD )
abstract class MultiDatabaseBoltStateMachineTestBase
{
    protected static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    protected static final String USER_AGENT = "BoltConnectionIT/0.0";
    protected static final BoltChannel BOLT_CHANNEL = BoltTestUtil.newTestBoltChannel( "conn-v0-test-boltchannel-id" );

    @RegisterExtension
    static final SessionExtension env = new SessionExtension( TestEnterpriseDatabaseManagementServiceBuilder::new );

    protected DatabaseManagementService managementService()
    {
        return env.managementService();
    }

    private String defaultDatabaseName()
    {
        return env.defaultDatabaseName();
    }

    @Test
    void shouldAllowSessionRunOnDefaultDatabase() throws Throwable
    {
        var machine = newStateMachineInReadyState();

        RecordedBoltResponse response = sessionRun( "Unwind [1, 2, 3] as n return n", machine );
        assertThat( response ).satisfies( containsRecord( 1L ) );
        reset( machine );
    }

    @Test
    void shouldAllowTxRunOnDefaultDatabase() throws Throwable
    {
        var machine = newStateMachineInReadyState();

        RecordedBoltResponse response = txRun( "Unwind [1, 2, 3] as n return n", machine );
        assertThat( response ).satisfies( containsRecord( 1L ) );
        reset( machine );
    }

    @Test
    void shouldAllowAnotherSessionRunAfterFailure() throws Throwable
    {
        // Given
        var machine = newStateMachineInReadyState();

        // When
        runWithFailure( "Invalid", machine, SyntaxError );
        reset( machine );

        // Then
        RecordedBoltResponse second = sessionRun( "Unwind [4, 5] as n return n", machine );
        assertThat( second ).satisfies( containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldAllowAnotherTxRunAfterFailure() throws Throwable
    {
        // Given
        var machine = newStateMachineInReadyState();

        // When
        runWithFailure( "Invalid", machine, SyntaxError );
        reset( machine );

        // Then
        RecordedBoltResponse second = txRun( "Unwind [4, 5] as n return n", machine );
        assertThat( second ).satisfies( containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldErrorIfDatabaseNotExists() throws Throwable
    {
        var machine = newStateMachineInReadyState();
        DatabaseManagementService managementService = managementService();
        managementService.dropDatabase( defaultDatabaseName() );
        runWithFailure( "RETURN 1", machine, Status.Database.DatabaseNotFound );
    }

    @Test
    void shouldErrorIfDatabaseStopped() throws Throwable
    {
        DatabaseManagementService managementService = managementService();
        managementService.shutdownDatabase( defaultDatabaseName() );

        var machine = newStateMachineInReadyState();
        runWithFailure( "RETURN 1", machine, Status.Database.DatabaseUnavailable );
    }

    @Test
    void shouldReportTxTerminationError() throws Throwable
    {
        // Given
        var machine = newStateMachineInReadyState();

        // When
        sessionBeginTx( machine );

        // Then we kill the current tx by marking it for termination
        machine.connectionState().getStatementProcessor().markCurrentTransactionForTermination();
        // We noticed that the tx is terminated.
        machine.validateTransaction();

        // Then
        runWithFailure( "RETURN 1", machine, Terminated );
    }

    @Test
    void shouldReportTxTerminationErrorWithoutValidate() throws Throwable
    {
        // Given
        var machine = newStateMachineInReadyState();

        // When
        sessionBeginTx( machine );

        // Then we kill the current tx by marking it for termination
        machine.connectionState().getStatementProcessor().markCurrentTransactionForTermination();

        // Then
        runWithFailure( "RETURN 1", machine, Terminated, false );
        // Explicit tx does not handle error, but rely on the client to either rollback or reset to release resources
        // If the user does not rollback or release, then machine.validateTx will kick in
        reset( machine );
    }

    /**
     * Simulate a driver session close
     */
    abstract void reset( AbstractBoltStateMachine machine ) throws Throwable;

    /**
     * Simulates a driver session run
     */
    abstract RecordedBoltResponse sessionRun( String query, AbstractBoltStateMachine machine ) throws Throwable;

    /**
     * Simulate a session or tx run failed with an error
     */
    abstract void runWithFailure( String query, AbstractBoltStateMachine machine, Status status, boolean isEmpty ) throws Throwable;

    /**
     * Simulates a driver tx run from beginTx to tx commit
     */
    abstract RecordedBoltResponse txRun( String query, AbstractBoltStateMachine machine ) throws Throwable;

    /**
     * Simulates a driver session beginTx
     */
    abstract void sessionBeginTx( AbstractBoltStateMachine machine ) throws Throwable;

    /**
     * Returns a {@link BoltStateMachine} for test
     */
    protected abstract AbstractBoltStateMachine newStateMachineInReadyState() throws Throwable;

    private void runWithFailure( String query, AbstractBoltStateMachine machine, Status status ) throws Throwable
    {
        runWithFailure( query, machine, status, true );
    }

    protected static void verifyStatementProcessor( AbstractBoltStateMachine machine, boolean isEmpty )
    {
        if ( isEmpty )
        {
            verifyStatementProcessorIsEmpty( machine );
        }
        else
        {
            verifyStatementProcessorNotEmpty( machine );
        }
    }

    /**
     * Verify the database reference is not set in the current connection context
     */
    protected static void verifyStatementProcessorIsEmpty( AbstractBoltStateMachine machine )
    {
        assertThat( machine.connectionState().getStatementProcessor() ).isEqualTo( EMPTY );
    }

    /**
     * Verify the database reference is set in the current connection context
     */
    protected static void verifyStatementProcessorNotEmpty( AbstractBoltStateMachine machine )
    {
        StatementProcessor processor = machine.connectionState().getStatementProcessor();
        assertThat( processor ).isNotEqualTo( EMPTY );
    }

    protected static HelloMessage newHelloMessage()
    {
        return new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) );
    }
}
