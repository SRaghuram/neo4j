/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.rules.RuleChain;

import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

public class StoreFilesWithRealFileSystemTest extends StoreFilesTest
{
    @Override
    protected void createRules()
    {
        testDirectory = TestDirectory.testDirectory();
        DefaultFileSystemRule defaultFileSystemRule = new DefaultFileSystemRule();
        fileSystemRule = defaultFileSystemRule;
        pageCacheRule = new PageCacheRule( );
        rules = RuleChain.outerRule( defaultFileSystemRule )
                         .around( testDirectory )
                         .around( pageCacheRule );
    }
}
