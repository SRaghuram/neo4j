/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.protocol.Protocol;

class CommercialCatchupV2Test extends CommercialCatchupTest
{
    CommercialCatchupV2Test()
    {
        super( Protocol.ApplicationProtocols.CATCHUP_2 );
    }

    @ParameterizedTest
    @MethodSource( "v2Tests" )
    void runTests( Function<DatabaseService,RequestResponse> scenario ) throws Exception
    {
        executeTestScenario( scenario );
    }

    static Stream<Function<DatabaseService,RequestResponse>> v2Tests()
    {
        return Stream.of( storeId(), wrongDb() );
    }
}
