/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

import static com.neo4j.server.enterprise.helpers.EnterpriseWebContainerBuilder.builderOnRandomPorts;

public abstract class EnterpriseWebContainerIT extends ExclusiveWebContainerTestBase
{
    @ClassRule
    public static final TestDirectory staticFolder = TestDirectory.testDirectory();
    protected static TestWebContainer testWebContainer;
    static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws Exception
    {
        testWebContainer = builderOnRandomPorts()
                                        .persistent()
                                        .usingDataDir( staticFolder.homeDir().getAbsolutePath() )
                                        .build();

        functionalTestHelper = new FunctionalTestHelper( testWebContainer );
    }

    @AfterClass
    public static void stopServer() throws Exception
    {
        testWebContainer.shutdown();
    }
}
