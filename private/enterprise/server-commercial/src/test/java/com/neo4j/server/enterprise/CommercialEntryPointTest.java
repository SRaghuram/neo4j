/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import java.io.PrintStream;

import com.neo4j.server.enterprise.CommercialEntryPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.commandline.Util.neo4jVersion;


public class CommercialEntryPointTest
{
    private PrintStream realSystemOut;
    private PrintStream fakeSystemOut;

    @Before
    public void setup()
    {
        realSystemOut = System.out;
        fakeSystemOut = mock( PrintStream.class );
        System.setOut( fakeSystemOut );
    }

    @After
    public void teardown()
    {
        System.setOut( realSystemOut );
    }

    @Test
    public void mainPrintsVersion() throws Exception
    {
        // when
        CommercialEntryPoint.main( new String[]{ "--version" } );

        // then
        verify( fakeSystemOut ).println( "neo4j " + neo4jVersion() );
        verifyNoMoreInteractions( fakeSystemOut );
    }
}
