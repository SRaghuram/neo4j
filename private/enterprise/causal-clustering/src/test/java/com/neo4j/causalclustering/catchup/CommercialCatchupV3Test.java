/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.protocol.Protocol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;

class CommercialCatchupV3Test extends CommercialCatchupTest
{
    CommercialCatchupV3Test()
    {
        super( Protocol.ApplicationProtocols.CATCHUP_3 );
    }

    @ParameterizedTest
    @MethodSource( "v3Tests" )
    void runTests( Function<DatabaseManager<? extends DatabaseContext>,RequestResponse> scenario ) throws Exception
    {
        executeTestScenario( scenario );
    }

    private static Stream<Function<DatabaseManager<? extends DatabaseContext>,RequestResponse>> v3Tests()
    {
        return Stream.of( storeId(), wrongDb() );
    }
}
