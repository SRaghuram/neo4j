/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension.dbms;

import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.fail;

@CommercialDbmsExtension( configurationCallback = "missing" )
public class CommercialDbmsExtensionEnforceAnnotations
{
    @SuppressWarnings( "unused" )
    void missing( TestDatabaseManagementServiceBuilder builder )
    {
    }

    @Test
    void missingExtensionAnnotation()
    {
        fail();
    }
}
