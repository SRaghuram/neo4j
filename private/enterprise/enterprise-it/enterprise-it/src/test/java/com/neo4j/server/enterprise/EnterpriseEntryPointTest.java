/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.internal.Version.getNeo4jVersion;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class EnterpriseEntryPointTest
{
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void mainPrintsVersion()
    {
        // when
        EnterpriseEntryPoint.main( new String[]{"--version" } );

        // then
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "neo4j " + getNeo4jVersion() ) );
    }
}
