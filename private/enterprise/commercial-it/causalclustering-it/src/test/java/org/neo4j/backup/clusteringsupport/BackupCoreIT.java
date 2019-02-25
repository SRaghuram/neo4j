/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.SuppressOutput;

import static com.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.backup.clusteringsupport.BackupUtil.backupArguments;
import static org.neo4j.backup.clusteringsupport.BackupUtil.createSomeData;
import static org.neo4j.backup.clusteringsupport.BackupUtil.getConfig;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class BackupCoreIT
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    private Cluster<?> cluster;
    private File backupsDir;

    @Before
    public void setup() throws Exception
    {
        backupsDir = clusterRule.testDirectory().cleanDirectory( "backups" );
        cluster = clusterRule.startCluster();
    }

    @Test
    public void makeSureBackupCanBePerformedFromAnyInstance() throws Throwable
    {
        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            // Run backup
            DbRepresentation beforeChange = DbRepresentation.of( createSomeData( cluster ) );
            String[] args = backupArguments( backupAddress( cluster ), backupsDir, "" + db.serverId() );
            assertEventually( () -> runBackupToolFromOtherJvmToGetExitCode( clusterRule.clusterDirectory(), args ), equalTo( 0 ), 5, TimeUnit.SECONDS );

            // Add some new data
            DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

            // Verify that old data is back
            DbRepresentation backupRepresentation = DbRepresentation.of( DatabaseLayout.of( backupsDir, "" + db.serverId() ).databaseDirectory(), getConfig() );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }

    private static String backupAddress( Cluster<?> cluster )
    {
        return cluster.getMemberWithRole( Role.LEADER ).settingValue( "causal_clustering.transaction_listen_address" );
    }

}
