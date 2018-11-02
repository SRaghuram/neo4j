/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel;

import org.junit.Test;

import org.neo4j.kernel.internal.Version;

import static org.junit.Assert.assertNotEquals;

public class VersionIT
{
    @Test
    public void canGetKernelRevision()
    {
        assertNotEquals( "Kernel revision not specified", "", Version.getKernelVersion() );
    }
}
