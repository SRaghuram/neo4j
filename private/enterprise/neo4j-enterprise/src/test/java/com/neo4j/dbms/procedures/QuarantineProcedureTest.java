/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.dbms.QuarantineOperator;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneId;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper;
import org.neo4j.time.FakeClock;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.stringValue;

class QuarantineProcedureTest
{
      @Test
      void placeDatabaseIntoQuarantine() throws ProcedureException
      {
          // given
          var operator = mock( QuarantineOperator.class );
          var databaseName = "foo";
          var reason = "Just a reason";
          var operatorResult = "OperatorResult";
          when( operator.putIntoQuarantine( anyString(), anyString() ) ).thenReturn( operatorResult );

          var fakeClock = FakeClock.fixed( Clock.systemUTC().instant(), ZoneId.systemDefault() );
          var time = ProceduresTimeFormatHelper.formatTime( fakeClock.instant(), ZoneId.systemDefault() );
          var message = String.format( "By  at %s: %s", time, reason );

          // when
          var result = callQuarantine( operator, fakeClock, GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
                  databaseName, true, reason );

          // then
          assertEquals( result, operatorResult );
          verify( operator ).putIntoQuarantine( databaseName, message );
      }

      @Test
      void shouldThrowOnNotSystem()
      {
          // given
          var operator = mock( QuarantineOperator.class );
          var databaseName = "foo";

          // when/then
          assertThrows( ProcedureException.class, () -> callQuarantine( operator, Clock.systemUTC(), databaseName,
                  databaseName, false, "does not matter" ) );
      }

    @Test
    void removeDatabaseFromQuarantine() throws ProcedureException
    {
        // given
        var operator = mock( QuarantineOperator.class );
        var databaseName = "foo";
        var operatorResult = "OperatorResult";
        when( operator.removeFromQuarantine( anyString() ) ).thenReturn( operatorResult );

        // when
        var result = callQuarantine( operator, Clock.systemUTC(), GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
                databaseName, false, "does not matter" );
        // then
        assertTrue( result.contains( operatorResult ) );
        verify( operator ).removeFromQuarantine( databaseName );
    }

    private String callQuarantine( QuarantineOperator operator, Clock clock, String actualDatabase, String databaseName, boolean setStatus, String reason )
            throws ProcedureException
    {
        var procedure = new QuarantineProcedure( operator, clock, ZoneId.systemDefault() );

        var context = mock( Context.class );
        var securityContext = mock( SecurityContext.class );
        var callContext = mock( ProcedureCallContext.class );
        when( context.securityContext() ).thenReturn( securityContext );
        when( context.procedureCallContext() ).thenReturn( callContext );
        when( callContext.databaseName() ).thenReturn( actualDatabase );
        when( callContext.isSystemDatabase() ).thenReturn( actualDatabase.equals( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) );
        when( securityContext.subject() ).thenReturn( AuthSubject.ANONYMOUS );

        var rawResult = procedure.apply( context, new AnyValue[]{stringValue( databaseName ), booleanValue( setStatus ), stringValue( reason )},
                mock( ResourceTracker.class ) );

        var resultRows = Iterators.asList( rawResult );
        assertEquals( 1, resultRows.size() );
        var resultRow = resultRows.get( 0 );
        assertEquals( databaseName, ((TextValue) resultRow[0]).stringValue() );
        assertEquals( setStatus, ((BooleanValue) resultRow[1]).booleanValue() );
        return ((TextValue) resultRow[2]).stringValue();
    }
}
