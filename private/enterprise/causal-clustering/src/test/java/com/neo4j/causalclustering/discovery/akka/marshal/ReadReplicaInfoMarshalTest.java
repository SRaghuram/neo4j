/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static org.junit.runners.Parameterized.Parameters;

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
        var dbIdRepo = new TestDatabaseIdRepository();
        return List.of(
                addressesForReadReplica( 42, Set.of() ),
                addressesForReadReplica( 789, Set.of( dbIdRepo.get( "db_one" ) ) ),
                addressesForReadReplica( 123, Set.of( dbIdRepo.get( "db_one" ), dbIdRepo.get( "db_two" ), dbIdRepo.get( "db_three" ) ) )
        );
    }
}

