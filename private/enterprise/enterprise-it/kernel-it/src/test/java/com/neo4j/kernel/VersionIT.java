/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.internal.Version;

class VersionIT
{
    @Test
    void canGetKernelRevision()
    {
        Assertions.assertNotEquals( "", Version.getKernelVersion(), "Kernel revision not specified" );
    }
}
