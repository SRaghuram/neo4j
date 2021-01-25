/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.dbms.database.DatabaseManager;

class EnterpriseCatchupV3Test extends EnterpriseCatchupTest
{
    EnterpriseCatchupV3Test()
    {
        super( ApplicationProtocols.CATCHUP_3_0 );
    }

    @ParameterizedTest
    @MethodSource( "v3Tests" )
    void runTests( Function<DatabaseManager<?>,RequestResponse> scenario ) throws Exception
    {
        executeTestScenario( scenario );
    }

    private static Stream<Function<DatabaseManager<?>,RequestResponse>> v3Tests()
    {
        return Stream.of( storeId(), wrongDb() );
    }
}
