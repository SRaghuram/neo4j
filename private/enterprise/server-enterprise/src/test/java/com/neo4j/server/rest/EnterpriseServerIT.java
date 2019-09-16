/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.concurrent.Callable;

import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public abstract class EnterpriseServerIT extends ExclusiveServerTestBase
{
    @ClassRule
    public static final TestDirectory staticFolder = TestDirectory.testDirectory();
    protected static NeoServer server;
    static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws Exception
    {
        server = EnterpriseServerBuilder.serverOnRandomPorts()
                                        .persistent()
                                        .usingDataDir( staticFolder.storeDir().getAbsolutePath() )
                                        .build();

        suppressAll().call((Callable<Void>) () ->
        {
            server.start();
            return null;
        });
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @AfterClass
    public static void stopServer() throws Exception
    {
        suppressAll().call((Callable<Void>) () ->
        {
            server.stop();
            return null;
        });
    }

    @Before
    public void setupTheDatabase()
    {
        // do nothing, we don't care about the database contents here
    }
}
