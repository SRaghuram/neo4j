/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.consistency;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class ConsistencyCheckServiceRecordFormatIT
{
    private final DbmsRule db = new EmbeddedDbmsRule()
            .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .startLazily();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( db );

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    @Before
    public void configureRecordFormat()
    {
        db.withSetting( GraphDatabaseSettings.record_format, recordFormat );
    }

    @Test
    public void checkTinyConsistentStore() throws Exception
    {
        db.ensureStarted();
        createLinkedList( db, 1_000 );
        db.shutdownAndKeepStore();

        assertConsistentStore( db );
    }

    private static void createLinkedList( GraphDatabaseService db, int size )
    {
        Node previous = null;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < size; i++ )
            {
                Label label = (i % 2 == 0) ? TestLabel.FOO : TestLabel.BAR;
                Node current = db.createNode( label );
                current.setProperty( "value", ThreadLocalRandom.current().nextLong() );

                if ( previous != null )
                {
                    previous.createRelationshipTo( current, TestRelType.FORWARD );
                    current.createRelationshipTo( previous, TestRelType.BACKWARD );
                }
                previous = current;
            }
            tx.success();
        }
    }

    private static void assertConsistentStore( GraphDatabaseAPI db ) throws Exception
    {
        ConsistencyCheckService service = new ConsistencyCheckService();

        ConsistencyCheckService.Result result = service.runFullConsistencyCheck( db.databaseLayout(), Config.defaults(),
                ProgressMonitorFactory.textual( System.out ), FormattedLogProvider.toOutputStream( System.out ), true );

        assertTrue( "Store is inconsistent", result.isSuccessful() );
    }

    private enum TestLabel implements Label
    {
        FOO,
        BAR
    }

    private enum TestRelType implements RelationshipType
    {
        FORWARD,
        BACKWARD
    }
}
