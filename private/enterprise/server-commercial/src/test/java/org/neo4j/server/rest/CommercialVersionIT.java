/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest;

import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import org.neo4j.server.NeoServer;
import org.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public abstract class CommercialVersionIT extends ExclusiveServerTestBase
{
    @ClassRule
    public static TemporaryFolder staticFolder = new TemporaryFolder();
    protected static NeoServer server;
    static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws Exception
    {
        FakeClock clock = Clocks.fakeClock();
        server = CommercialServerBuilder.serverOnRandomPorts()
                .usingDataDir( staticFolder.getRoot().getAbsolutePath() )
                .withClock(clock)
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
    public void setupTheDatabase() throws Exception
    {
        // do nothing, we don't care about the database contents here
    }
}
