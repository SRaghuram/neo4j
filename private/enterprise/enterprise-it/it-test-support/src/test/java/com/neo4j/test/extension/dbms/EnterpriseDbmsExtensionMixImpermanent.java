/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

@ImpermanentEnterpriseDbmsExtension
public class EnterpriseDbmsExtensionMixImpermanent
{
    @Test
    void mixedExtensionAnnotation()
    {
    }

    @Nested
    @EnterpriseDbmsExtension
    public class NestedTest
    {
        @Test
        void mixed()
        {
            fail();
        }
    }
}
