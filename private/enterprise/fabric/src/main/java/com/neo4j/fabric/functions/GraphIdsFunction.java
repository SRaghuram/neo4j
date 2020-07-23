/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.functions;

import com.neo4j.configuration.FabricEnterpriseConfig;

import java.util.Collections;
import java.util.List;

import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

public class GraphIdsFunction implements CallableUserFunction
{
    private static final String NAME = "graphIds";
    private static final String DESCRIPTION = "List all fabric graph ids";
    private static final String CATEGORY = "List";
    private static final List<FieldSignature> ARGUMENT_TYPES = Collections.emptyList();
    private static final Neo4jTypes.ListType RESULT_TYPE = Neo4jTypes.NTList( Neo4jTypes.NTInteger );

    private final UserFunctionSignature signature;
    private final FabricEnterpriseConfig fabricConfig;

    public GraphIdsFunction( FabricEnterpriseConfig fabricConfig )
    {
        this.fabricConfig = fabricConfig;
        String namespace = fabricConfig.getDatabase().getName().name();
        this.signature = new UserFunctionSignature(
                new QualifiedName( new String[]{namespace}, NAME ),
                ARGUMENT_TYPES, RESULT_TYPE, null, new String[0],
                DESCRIPTION, CATEGORY, true );
    }

    @Override
    public UserFunctionSignature signature()
    {
        return signature;
    }

    @Override
    public boolean threadSafe()
    {
        return true;
    }

    @Override
    public AnyValue apply( Context ctx, AnyValue[] input )
    {
        return Values.longArray(
                fabricConfig.getDatabase().getGraphs().stream()
                        .mapToLong( FabricEnterpriseConfig.Graph::getId )
                        .toArray()
        );
    }
}
