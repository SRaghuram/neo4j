/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static java.util.Collections.disjoint;
import static java.util.stream.Collectors.toList;
import static org.eclipse.collections.impl.factory.Sets.intersect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;

@CommercialDbmsExtension
class PrepareStoreCopyFilesIT
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldReturnDifferentAtomicAndReplayableFiles() throws Exception
    {
        try ( var prepareStoreCopyFiles = newPrepareStoreCopyFiles( db ) )
        {
            createSchemaAndData( db );

            var atomicFiles = atomicFiles( prepareStoreCopyFiles );
            var replayableFiles = replayableFiles( prepareStoreCopyFiles );

            assertTrue( disjoint( atomicFiles, replayableFiles ),
                    () -> "Atomic and replayable files contain same elements:\n" + intersect( atomicFiles, replayableFiles ) );
        }
    }

    private static PrepareStoreCopyFiles newPrepareStoreCopyFiles( GraphDatabaseAPI db )
    {
        var resolver = db.getDependencyResolver();
        return new PrepareStoreCopyFiles( resolver.resolveDependency( Database.class ), resolver.resolveDependency( FileSystemAbstraction.class ) );
    }

    private static Set<File> atomicFiles( PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        var atomicFilesList = Arrays.stream( prepareStoreCopyFiles.getAtomicFilesSnapshot() )
                .map( StoreResource::file )
                .collect( toList() );

        var atomicFilesSet = Set.copyOf( atomicFilesList );
        assertEquals( atomicFilesList.size(), atomicFilesSet.size() );
        return atomicFilesSet;
    }

    private static Set<File> replayableFiles( PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        var replayableFilesArray = prepareStoreCopyFiles.listReplayableFiles();
        var replayableFilesSet = Set.of( replayableFilesArray );
        assertEquals( replayableFilesArray.length, replayableFilesSet.size() );
        return replayableFilesSet;
    }

    private static void createSchemaAndData( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            db.execute( "CREATE INDEX ON :Person(id)" ).close();
            db.execute( "CALL db.createIndex(':Person(name)', $provider)", Map.of( "provider", NATIVE30.providerName() ) ).close();
            db.execute( "CALL db.index.fulltext.createNodeIndex('nameAndTitle', ['Person'], ['name', 'title'])" ).close();
            db.execute( "CALL db.index.fulltext.createRelationshipIndex('description', ['KNOWS'], ['description'])" ).close();
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            db.execute( "CALL db.awaitIndexes(60)" ).close();

            db.execute( "UNWIND range(1, 100) AS x " +
                    "WITH x AS x, x + 1 AS y " +
                    "CREATE (p1:Person {id: x, name: 'name-' + x, title: 'title-' + x})," +
                    "       (p2:Person {id: y, name: 'name-' + y, title: 'title-' + y})," +
                    "       (p1)-[:KNOWS {description: 'description-' + x + '-' + y}]->(p2)" ).close();

            db.execute( "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()" ).close();
            transaction.commit();
        }
    }
}
