/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit.extension;

import org.neo4j.harness.junit.extension.Neo4jExtensionBuilder;

import static com.neo4j.harness.internal.CommercialTestNeo4jBuilders.newInProcessBuilder;

/**
 * {@link CommercialNeo4jExtension} extension builder.
 */
class CommercialNeo4jExtensionBuilder extends Neo4jExtensionBuilder
{
    CommercialNeo4jExtensionBuilder()
    {
        super( newInProcessBuilder() );
    }
}
