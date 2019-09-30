/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.neo4j.dbms.DatabaseState;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

@RunWith( Parameterized.class )
public class DatabaseStateMarshalTest extends BaseMarshalTest<DatabaseState>
{

    public DatabaseStateMarshalTest( DatabaseState original )
    {
        super( original, DatabaseStateMarshal.INSTANCE );
    }

    @Parameterized.Parameters
    public static Collection<DatabaseState> parameters()
    {
        return Arrays.stream( EnterpriseOperatorState.values() )
                .map( state -> new EnterpriseDatabaseState( randomDatabaseId(), state ) )
                .collect( Collectors.toList() );
    }
}
