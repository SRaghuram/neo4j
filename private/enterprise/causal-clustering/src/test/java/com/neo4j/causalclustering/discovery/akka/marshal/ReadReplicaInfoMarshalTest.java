/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

@RunWith( Parameterized.class )
public class ReadReplicaInfoMarshalTest extends BaseMarshalTest<ReadReplicaInfo>
{
    public ReadReplicaInfoMarshalTest( ReadReplicaInfo info )
    {
        super( info, new ReadReplicaInfoMarshal() );
    }

    @Parameters( name = "{0}" )
    public static Collection<ReadReplicaInfo> data()
    {
        return List.of(
                addressesForReadReplica( 42, Set.of() ),
                addressesForReadReplica( 789, Set.of( randomDatabaseId() ) ),
                addressesForReadReplica( 123, Set.of( randomDatabaseId(), randomDatabaseId(), randomDatabaseId() ) )
        );
    }
}

