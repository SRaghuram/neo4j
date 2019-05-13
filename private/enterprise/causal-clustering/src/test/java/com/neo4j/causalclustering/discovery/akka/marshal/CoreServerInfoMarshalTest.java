/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;

@RunWith( Parameterized.class )
public class CoreServerInfoMarshalTest extends BaseMarshalTest<CoreServerInfo>
{
    public CoreServerInfoMarshalTest( CoreServerInfo info )
    {
        super( info, new CoreServerInfoMarshal() );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<CoreServerInfo> data()
    {
        var dbIdRepo = new TestDatabaseIdRepository();
        return List.of(
                addressesForCore( 42, false, Set.of() ),
                addressesForCore( 4242, true, Set.of() ),
                addressesForCore( 513, false, Set.of( dbIdRepo.get( "db_one" ) ) ),
                addressesForCore( 98738, true, Set.of( dbIdRepo.get( "db_one" ) ) ),
                addressesForCore( 145, false, Set.of( dbIdRepo.get( "db_one" ), dbIdRepo.get( "db_two" ), dbIdRepo.get( "db_three" ) ) ),
                addressesForCore( 8361, true, Set.of( dbIdRepo.get( "db_one" ), dbIdRepo.get( "db_two" ), dbIdRepo.get( "db_three" ) ) )
        );
    }
}
