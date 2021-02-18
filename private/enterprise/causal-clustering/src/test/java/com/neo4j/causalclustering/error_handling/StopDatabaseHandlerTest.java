/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.dbms.ClusterInternalDbmsOperator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class StopDatabaseHandlerTest
{

    private ClusterInternalDbmsOperator operator = mock( ClusterInternalDbmsOperator.class );
    private StopDatabaseHandler handler = new StopDatabaseHandler( operator );
    private NamedDatabaseId namedDatabaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );

    @Test
    void panicMessageShouldConsistOfReasonDescriptionAndCauseMessage()
    {
        final var reason = DatabasePanicReason.CATCHUP_FAILED;
        final var ioMessage = "file not found";
        final var ioException = new IOException( ioMessage );

        //when/then
        final var panicMessage = handler.getPanicMessage( new DatabasePanicEvent( namedDatabaseId, reason, ioException ) );
        assertThat( panicMessage ).isEqualTo( reason.getDescription() + ": " + ioMessage );
    }

    @Test
    void panicMessageShouldConsistOfReasonDescriptionWhenCauseIsNull()
    {
        final var reason = DatabasePanicReason.CATCHUP_FAILED;
        final var ioException = new IOException( (Throwable) null );

        //when/then
        final var panicMessage = handler.getPanicMessage( new DatabasePanicEvent( namedDatabaseId, reason, ioException ) );
        assertThat( panicMessage ).isEqualTo( reason.getDescription() );
    }

    @Test
    void ifCauseMessageIsNullThenSubCauseMessageShouldBeUsedAsAPanicMessage()
    {
        final var reason = DatabasePanicReason.CATCHUP_FAILED;
        final var ioMessage = "file not found";
        final var ioException = new IOException( null, new IllegalStateException( null, new IllegalStateException( ioMessage ) ) );

        //when/then
        final var panicMessage = handler.getPanicMessage( new DatabasePanicEvent( namedDatabaseId, reason, ioException ) );
        assertThat( panicMessage ).isEqualTo( reason.getDescription() + ": " + ioMessage );
    }

    @Test
    void causeMessageShouldBeUsedEvenIfSubCauseIsNull()
    {
        final var reason = DatabasePanicReason.CATCHUP_FAILED;
        final var ioMessage = "file not found";
        final var ioException = new IOException( ioMessage, null );

        //when/then
        final var panicMessage = handler.getPanicMessage( new DatabasePanicEvent( namedDatabaseId, reason, ioException ) );
        assertThat( panicMessage ).isEqualTo( reason.getDescription() + ": " + ioMessage );
    }
}
