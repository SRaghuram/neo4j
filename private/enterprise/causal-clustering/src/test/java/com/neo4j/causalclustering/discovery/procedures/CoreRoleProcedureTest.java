/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.core.IdentityModule;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.values.AnyValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

class CoreRoleProcedureTest
{
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final DatabaseId databaseId = databaseIdRepository.getByName( "cars" ).get();
    private final MemberId memberId = new MemberId( UUID.randomUUID() );
    private final IdentityModule identityModule = mock( IdentityModule.class );
    private final TopologyService topologyService = mock( TopologyService.class );

    private final Context procedureContext = mock( Context.class );
    private final ResourceTracker resourceTracker = mock( ResourceTracker.class );

    private final CoreRoleProcedure procedure = new CoreRoleProcedure( identityModule, topologyService, databaseIdRepository );

    @Test
    void shouldThrowWhenDatabaseNameNotSpecified()
    {
        assertThrows( IllegalArgumentException.class, () -> procedure.apply( procedureContext, new AnyValue[]{}, resourceTracker ) );
    }

    @Test
    void shouldThrowWhenDatabaseNameIsNull()
    {
        assertThrows( IllegalArgumentException.class, () -> procedure.apply( procedureContext, new AnyValue[]{null}, resourceTracker ) );
    }

    @Test
    void shouldThrowWhenDatabaseNameIsNotString()
    {
        assertThrows( IllegalArgumentException.class, () -> procedure.apply( procedureContext, new AnyValue[]{intValue( 42 )}, resourceTracker ) );
    }

    @Test
    void shouldReturnLeader() throws Exception
    {
        testProcedureCall( RoleInfo.LEADER );
    }

    @Test
    void shouldReturnFollower() throws Exception
    {
        testProcedureCall( RoleInfo.FOLLOWER );
    }

    @Test
    void shouldReturnUnknown() throws Exception
    {
        testProcedureCall( RoleInfo.UNKNOWN );
    }

    @Test
    void shouldHaveCorrectName()
    {
        assertEquals( new QualifiedName( List.of( "dbms", "cluster" ), "role" ), procedure.signature().name() );
    }

    @Test
    void shouldBeASystemProcedure()
    {
        assertTrue( procedure.signature().systemProcedure() );
    }

    private void testProcedureCall( RoleInfo role ) throws Exception
    {
        when( identityModule.myself() ).thenReturn( memberId );
        when( topologyService.coreRole( databaseId, memberId ) ).thenReturn( role );

        var result = procedure.apply( procedureContext, new AnyValue[]{stringValue( databaseId.name() )}, resourceTracker );
        assertTrue( result.hasNext() );

        var row = result.next();
        assertFalse( result.hasNext() );
        assertEquals( 1, row.length );

        var element = row[0];
        assertEquals( stringValue( role.toString() ), element );
    }
}
