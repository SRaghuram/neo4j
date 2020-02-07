/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.test.extension.DbmsSupportController;
import org.neo4j.test.extension.DbmsSupportExtension;

public class EnterpriseDbmsSupportExtension extends DbmsSupportExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback
{
    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        EnterpriseDbmsSupportController controller = new EnterpriseDbmsSupportController( context );
        controller.startDbms();
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        DbmsSupportController.remove( context ).shutdown();
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        // Create a new database for each test method
        DbmsSupportController controller = DbmsSupportController.get( context );
        String uniqueTestName = getUniqueTestName( context );
        controller.startDatabase( uniqueTestName );
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        DbmsSupportController.get( context ).stopDatabase();
    }

    private static String getUniqueTestName( ExtensionContext context )
    {
        // Test name is restricted to 31 characters since database name must be less than or equal to 63 characters
        String testName = context.getRequiredTestMethod().getName();
        testName = testName.substring( 0, Math.min( 31, testName.length() ) );
        return testName + DigestUtils.md5Hex( context.getUniqueId() );
    }
}
