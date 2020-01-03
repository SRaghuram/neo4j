/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseToMember;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

@RunWith( Parameterized.class )
public class DatabaseToMemberMarshalTest extends BaseMarshalTest<DatabaseToMember>
{
    public DatabaseToMemberMarshalTest( DatabaseToMember original )
    {
        super( original, DatabaseToMemberMarshal.INSTANCE );
    }

    @Parameterized.Parameters
    public static Collection<DatabaseToMember> parameters()
    {
        return Stream.generate( () -> new DatabaseToMember( randomDatabaseId(), new MemberId( UUID.randomUUID() ) ) )
                .limit( 5 )
                .collect( Collectors.toList() );
    }
}
