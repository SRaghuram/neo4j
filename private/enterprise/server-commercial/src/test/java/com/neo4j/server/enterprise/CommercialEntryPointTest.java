/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.commandline.Util.neo4jVersion;

@ExtendWith( SuppressOutputExtension.class )
class CommercialEntryPointTest
{
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void mainPrintsVersion()
    {
        // when
        CommercialEntryPoint.main( new String[]{ "--version" } );

        // then
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "neo4j " + neo4jVersion() ) );
    }
}
