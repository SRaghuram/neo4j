/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.builtin.BuiltInProcedures;
import org.neo4j.procedure.builtin.IndexProcedures;

import static org.neo4j.procedure.Mode.SCHEMA;

@SuppressWarnings( "unused" )
public class EnterpriseBuiltInProcedures
{
    @Context
    public KernelTransaction tx;

    @Context
    public DependencyResolver resolver;

    @Description( "Create a named node key constraint with index backed by specified index provider. " +
            "The optional 'config' parameter can be used to supply settings to the index. Config settings are submitted as a map. " +
            "Note that settings keys might need to be escaped with back-ticks, " +
            "config example: {`spatial.cartesian.maxLevels`: 5, `spatial.cartesian.min`: [-45.0, -45.0]}. " +
            "Example: CALL db.createNodeKey(\"MyConstraint\", [\"Person\"], [\"name\"], \"native-btree-1.0\") - " +
            "YIELD name, labels, properties, providerName, status" )
    @Procedure( name = "db.createNodeKey", mode = SCHEMA )
    public Stream<BuiltInProcedures.SchemaIndexInfo> createNodeKey(
            @Name( "constraintName" ) String constraintName,
            @Name( "labels" ) List<String> labels,
            @Name( "properties" ) List<String> properties,
            @Name( "providerName" ) String providerName,
            @Name( value = "config", defaultValue = "{}" ) Map<String,Object> config )
            throws ProcedureException
    {
        IndexProcedures indexProcedures = indexProcedures();
        final IndexProviderDescriptor indexProviderDescriptor = getIndexProviderDescriptor( providerName );
        return indexProcedures.createNodeKey( constraintName, labels, properties, indexProviderDescriptor, config );
    }

    private IndexProviderDescriptor getIndexProviderDescriptor( String providerName )
    {
        return resolver.resolveDependency( IndexingService.class ).indexProviderByName( providerName );
    }

    private IndexProcedures indexProcedures()
    {
        return new IndexProcedures( tx, resolver.resolveDependency( IndexingService.class ) );
    }
}
