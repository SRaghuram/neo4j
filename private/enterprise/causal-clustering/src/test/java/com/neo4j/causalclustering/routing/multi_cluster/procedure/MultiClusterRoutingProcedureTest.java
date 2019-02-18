/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;

import static org.junit.Assert.assertEquals;

public class MultiClusterRoutingProcedureTest
{

    @Test
    public void subClusterRoutingProcedureShouldHaveCorrectSignature()
    {
        GetRoutersForDatabaseProcedure proc = new GetRoutersForDatabaseProcedure( null, Config.defaults() );

        ProcedureSignature procSig = proc.signature();

        List<FieldSignature> input = Collections.singletonList( FieldSignature.inputField( "database", Neo4jTypes.NTString ) );
        List<FieldSignature> output = Arrays.asList(
                FieldSignature.outputField( "ttl", Neo4jTypes.NTInteger ),
                FieldSignature.outputField( "routers", Neo4jTypes.NTList( Neo4jTypes.NTMap ) ) );

        assertEquals( "The input signature of the GetRoutersForDatabaseProcedure should not change.", procSig.inputSignature(), input );

        assertEquals( "The output signature of the GetRoutersForDatabaseProcedure should not change.", procSig.outputSignature(), output );
    }

    @Test
    public void superClusterRoutingProcedureShouldHaveCorrectSignature()
    {
        GetRoutersForAllDatabasesProcedure proc = new GetRoutersForAllDatabasesProcedure( null, Config.defaults() );

        ProcedureSignature procSig = proc.signature();

        List<FieldSignature> output = Arrays.asList(
                FieldSignature.outputField( "ttl", Neo4jTypes.NTInteger ),
                FieldSignature.outputField( "routers", Neo4jTypes.NTList( Neo4jTypes.NTMap ) ) );

        assertEquals( "The output signature of the GetRoutersForAllDatabasesProcedure should not change.", procSig.outputSignature(), output );
    }
}
