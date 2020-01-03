/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

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
        return List.of(
                addressesForCore( 42, false, Set.of() ),
                addressesForCore( 4242, true, Set.of() ),
                addressesForCore( 513, false, Set.of( randomDatabaseId() ) ),
                addressesForCore( 98738, true, Set.of( randomDatabaseId() ) ),
                addressesForCore( 145, false, Set.of( randomDatabaseId(), randomDatabaseId(), randomDatabaseId() ) ),
                addressesForCore( 8361, true, Set.of( randomDatabaseId(), randomDatabaseId(), randomDatabaseId() ) )
        );
    }
}
