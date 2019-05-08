/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension.dbms;

import com.neo4j.test.extension.CommercialDbmsExtension;
import com.neo4j.test.extension.ImpermanentCommercialDbmsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

@ImpermanentCommercialDbmsExtension
public class CommercialDbmsExtensionMixImpermanent
{
    @Test
    void mixedExtensionAnnotation()
    {
    }

    @Nested
    @CommercialDbmsExtension
    public class NestedTest
    {
        @Test
        void mixed()
        {
            fail();
        }
    }
}
