/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.util.function.Function;
import java.util.stream.Stream;

import com.neo4j.causalclustering.protocol.Protocol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.neo4j.dbms.database.DatabaseManager;

class CommercialCatchupV2Test extends CommercialCatchupTest
{
    CommercialCatchupV2Test()
    {
        super( Protocol.ApplicationProtocols.CATCHUP_2 );
    }

    @ParameterizedTest
    @MethodSource( "v2Tests" )
    void runTests( Function<DatabaseManager,RequestResponse> scenario ) throws Exception
    {
        executeTestScenario( scenario );
    }

    static Stream<Function<DatabaseManager,RequestResponse>> v2Tests()
    {
        return Stream.of( storeId(), wrongDb() );
    }
}
