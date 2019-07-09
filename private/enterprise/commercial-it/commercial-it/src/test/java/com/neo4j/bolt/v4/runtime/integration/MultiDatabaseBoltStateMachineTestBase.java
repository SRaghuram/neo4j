/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.v4.runtime.integration;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.runtime.BoltStateMachineV1;
import org.neo4j.bolt.v1.runtime.integration.SessionExtension;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.runtime.StatementProcessor.EMPTY;
import static org.neo4j.bolt.testing.BoltMatchers.containsRecord;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

abstract class MultiDatabaseBoltStateMachineTestBase
{
    protected static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    protected static final String USER_AGENT = "BoltConnectionIT/0.0";
    protected static final BoltChannel BOLT_CHANNEL = BoltTestUtil.newTestBoltChannel( "conn-v0-test-boltchannel-id" );

    @RegisterExtension
    static final SessionExtension env = new SessionExtension( TestCommercialDatabaseManagementServiceBuilder::new );

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
        BoltStateMachineV1 machine = newStateMachineInReadyState();

        RecordedBoltResponse response = sessionRun( "Unwind [1, 2, 3] as n return n", machine );
        assertThat( response, containsRecord( 1L ) );
        reset( machine );
    }

    @Test
    void shouldAllowTxRunOnDefaultDatabase() throws Throwable
    {
        BoltStateMachineV1 machine = newStateMachineInReadyState();

        RecordedBoltResponse response = txRun( "Unwind [1, 2, 3] as n return n", machine );
        assertThat( response, containsRecord( 1L ) );
        reset( machine );
    }

    @Test
    void shouldAllowAnotherSessionRunAfterFailure() throws Throwable
    {
        // Given
        BoltStateMachineV1 machine = newStateMachineInReadyState();

        // When
        runWithFailure( "Invalid", machine, SyntaxError );
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
        BoltStateMachineV1 machine = newStateMachineInReadyState();

        // When
        runWithFailure( "Invalid", machine, SyntaxError );
        reset( machine );

        // Then
        RecordedBoltResponse second = txRun( "Unwind [4, 5] as n return n", machine );
        assertThat( second, containsRecord( 4L ) );
        reset( machine );
    }

    @Test
    void shouldErrorIfDatabaseNotExists() throws Throwable
    {
        BoltStateMachineV1 machine = newStateMachineInReadyState();
        DatabaseManagementService managementService = managementService();
        managementService.dropDatabase( defaultDatabaseName() );
        runWithFailure( "RETURN 1", machine, Status.Database.DatabaseNotFound );
    }

    @Test
    void shouldErrorIfDatabaseStopped() throws Throwable
    {
        DatabaseManagementService managementService = managementService();
        managementService.shutdownDatabase( defaultDatabaseName() );

        BoltStateMachineV1 machine = newStateMachineInReadyState();
        runWithFailure( "RETURN 1", machine, Status.General.DatabaseUnavailable );
    }

    @Test
    void shouldReportTxTerminationError() throws Throwable
    {
        // Given
        BoltStateMachineV1 machine = newStateMachineInReadyState();

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
        BoltStateMachineV1 machine = newStateMachineInReadyState();

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
    abstract void reset( BoltStateMachineV1 machine ) throws Throwable;

    /**
     * Simulates a driver session run
     */
    abstract RecordedBoltResponse sessionRun( String query, BoltStateMachineV1 machine ) throws Throwable;

    /**
     * Simulate a session or tx run failed with an error
     */
    abstract void runWithFailure( String query, BoltStateMachineV1 machine, Status status, boolean isEmpty ) throws Throwable;

    /**
     * Simulates a driver tx run from beginTx to tx commit
     */
    abstract RecordedBoltResponse txRun( String query, BoltStateMachineV1 machine ) throws Throwable;

    /**
     * Simulates a driver session beginTx
     */
    abstract void sessionBeginTx( BoltStateMachineV1 machine ) throws Throwable;

    /**
     * Returns a {@link BoltStateMachine} for test
     */
    protected abstract BoltStateMachineV1 newStateMachineInReadyState() throws Throwable;

    private void runWithFailure( String query, BoltStateMachineV1 machine, Status status ) throws Throwable
    {
        runWithFailure( query, machine, status, true );
    }

    protected static void verifyStatementProcessor( BoltStateMachineV1 machine, boolean isEmpty )
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
    protected static void verifyStatementProcessorIsEmpty( BoltStateMachineV1 machine )
    {
        assertThat( machine.connectionState().getStatementProcessor(), equalTo( EMPTY ) );
    }

    /**
     * Verify the database reference is set in the current connection context
     */
    protected static void verifyStatementProcessorNotEmpty( BoltStateMachineV1 machine )
    {
        StatementProcessor processor = machine.connectionState().getStatementProcessor();
        assertThat( processor, not( EMPTY ) );
    }

    protected static HelloMessage newHelloMessage()
    {
        return new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) );
    }
}
