/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import org.assertj.core.api.Condition;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static com.neo4j.backup.BackupTestUtil.restoreFromBackup;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

class ReplaceRandomMember extends RepeatOnRandomMember
{
    /* Basic pass criteria for the stress test. We must have replaced at least two members. */
    private static final int MIN_SUCCESSFUL_REPLACEMENTS = 1;

    /* Backups retry a few times with a pause in between. */
    private static final long MAX_BACKUP_FAILURES = 20;
    private static final long RETRY_TIMEOUT_MILLIS = 5000;

    private final Cluster cluster;
    private final FileSystemAbstraction fs;
    private final Log log;
    private final BackupHelper backupHelper;

    private int successfulReplacements;

    ReplaceRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.cluster = resources.cluster();
        this.backupHelper = new BackupHelper( resources );
        this.fs = resources.fileSystem();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    public void doWorkOnMember( ClusterMember oldMember ) throws Exception
    {
        boolean replaceFromBackup = ThreadLocalRandom.current().nextBoolean();

        Path backup = null;
        if ( replaceFromBackup )
        {
            backup = createBackupWithRetries( oldMember );
        }

        log.info( "Stopping: " + oldMember );
        oldMember.shutdown();

        boolean replacingCore = oldMember instanceof CoreClusterMember;
        ClusterMember newMember = replacingCore ? cluster.newCoreMember() : cluster.newReadReplica();

        if ( replaceFromBackup )
        {
            log.info( "Restoring backup: " + backup.getFileName().toString() + " to: " + newMember );
            restoreFromBackup( backup, fs, newMember, DEFAULT_DATABASE_NAME );
            fs.deleteRecursively( backup );
        }

        log.info( "Starting: " + newMember );
        newMember.start();

        if ( replacingCore )
        {
            awaitRaftMembership( (CoreClusterMember) newMember );
        }

        successfulReplacements++;
    }

    private void awaitRaftMembership( CoreClusterMember core )
    {
        var databaseNames = core.managementService().listDatabases();

        for ( String databaseName : databaseNames )
        {
            log.info( format( "Waiting for membership of '%s'", databaseName ) );
            var raft = core.resolveDependency( databaseName, RaftMachine.class );
            assertEventually(  members -> format( "Voting members %s do not contain %s", members, raft.memberId() ),
                    raft::votingMembers, new Condition<>( memberIds -> memberIds.contains( raft.memberId() ), "Contains core id." ), 10, MINUTES );
        }
    }

    private Path createBackupWithRetries( ClusterMember member ) throws Exception
    {
        int failureCount = 0;

        while ( true )
        {
            Optional<Path> backupOpt = backupHelper.backup( member );
            if ( backupOpt.isPresent() )
            {
                return backupOpt.get();
            }
            else
            {
                failureCount++;

                if ( failureCount >= MAX_BACKUP_FAILURES )
                {
                    throw new RuntimeException( format( "Backup failed %s times in a row.", failureCount ) );
                }

                log.info( "Retrying backup in %s ms.", RETRY_TIMEOUT_MILLIS );
                Thread.sleep( RETRY_TIMEOUT_MILLIS );
            }
        }
    }

    @Override
    public void validate()
    {
        assertThat( successfulReplacements ).isGreaterThanOrEqualTo( MIN_SUCCESSFUL_REPLACEMENTS );
    }
}
