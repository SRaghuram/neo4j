/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import org.neo4j.causalclustering.error_handling.Panicker;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.IdGeneratorContractTest;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.max;
import static java.util.Collections.min;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
public class ReplicatedIdGeneratorTest extends IdGeneratorContractTest
{
    private NullLogProvider logProvider = NullLogProvider.getInstance();

    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    private File file;
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private RaftMachine raftMachine = Mockito.mock( RaftMachine.class );
    private ExposedRaftState state = mock( ExposedRaftState.class );
    private final CommandIndexTracker commandIndexTracker = mock( CommandIndexTracker.class );
    private IdReusabilityCondition idReusabilityCondition;
    private ReplicatedIdGenerator idGenerator;
    private Panicker panicker;

    @BeforeEach
    void setUp()
    {
        file = testDirectory.file( "idgen" );
        when( raftMachine.state() ).thenReturn( state );
        idReusabilityCondition = getIdReusabilityCondition();
        panicker = mock( Panicker.class );
    }

    @AfterEach
    void tearDown()
    {
        if ( idGenerator != null )
        {
            idGenerator.close();
        }
    }

    @Override
    protected IdGenerator createIdGenerator( int grabSize )
    {
        return openIdGenerator( grabSize );
    }

    @Override
    protected IdGenerator openIdGenerator( int grabSize )
    {
        ReplicatedIdGenerator replicatedIdGenerator = getReplicatedIdGenerator( grabSize, 0L, stubAcquirer() );
        return new FreeIdFilteredIdGenerator( replicatedIdGenerator, idReusabilityCondition );
    }

    @Test
    void shouldCreateIdFileForPersistence()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        idGenerator = getReplicatedIdGenerator( 10, 0L, rangeAcquirer );

        assertTrue( fs.fileExists( file ) );
    }

    @Test
    void shouldNotStepBeyondAllocationBoundaryWithoutBurnedId()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        idGenerator = getReplicatedIdGenerator( 10, 0L, rangeAcquirer );

        Set<Long> idsGenerated = collectGeneratedIds( idGenerator, 1024 );

        long minId = min( idsGenerated );
        long maxId = max( idsGenerated );

        assertEquals( 0L, minId );
        assertEquals( 1023L, maxId );
    }

    @Test
    void shouldNotStepBeyondAllocationBoundaryWithBurnedId()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        long burnedIds = 23L;
        idGenerator = getReplicatedIdGenerator( 10, burnedIds, rangeAcquirer );

        Set<Long> idsGenerated = collectGeneratedIds( idGenerator, 1024 - burnedIds );

        long minId = min( idsGenerated );
        long maxId = max( idsGenerated );

        assertEquals( burnedIds, minId );
        assertEquals( 1023, maxId );
    }

    @Test
    void shouldThrowAndPanicIfAdjustmentFailsDueToInconsistentValues()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        when( rangeAcquirer.acquireIds( IdType.NODE ) ).thenReturn( allocation( 3, 21, 21 ) );
        idGenerator = getReplicatedIdGenerator( 10, 42L, rangeAcquirer );

        assertThrows( IdAllocationException.class, () -> idGenerator.nextId() );

        verify( panicker, times( 1 ) ).panic( Matchers.isA( IdAllocationException.class ) );
    }

    @Test
    void shouldReuseIdOnlyWhenLeader()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        long burnedIds = 23L;
        try ( FreeIdFilteredIdGenerator idGenerator = new FreeIdFilteredIdGenerator( getReplicatedIdGenerator( 10, burnedIds, rangeAcquirer ),
                idReusabilityCondition ) )
        {

            idGenerator.freeId( 10 );
            assertEquals( 0, idGenerator.getDefragCount() );
            assertEquals( 23, idGenerator.nextId() );

            when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( 6L ); // gap-free
            when( state.lastLogIndexBeforeWeBecameLeader() ).thenReturn( 5L );
            idReusabilityCondition.onLeaderSwitch( new LeaderInfo( myself, 1 ) );

            idGenerator.freeId( 10 );
            assertEquals( 1, idGenerator.getDefragCount() );
            assertEquals( 10, idGenerator.nextId() );
            assertEquals( 0, idGenerator.getDefragCount() );
        }
    }

    @Test
    void shouldReuseIdBeforeHighId()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        long burnedIds = 23L;
        idGenerator = getReplicatedIdGenerator( 10, burnedIds, rangeAcquirer );

        assertEquals( 23, idGenerator.nextId() );

        idGenerator.freeId( 10 );
        idGenerator.freeId( 5 );

        assertEquals( 10, idGenerator.nextId() );
        assertEquals( 5, idGenerator.nextId() );
        assertEquals( 24, idGenerator.nextId() );
    }

    @Test
    void freeIdOnlyWhenReusabilityConditionAllows()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        IdReusabilityCondition idReusabilityCondition = getIdReusabilityCondition();

        long burnedIds = 23L;
        try ( FreeIdFilteredIdGenerator idGenerator = new FreeIdFilteredIdGenerator( getReplicatedIdGenerator( 10, burnedIds, rangeAcquirer ),
                idReusabilityCondition ) )
        {

            idGenerator.freeId( 10 );
            assertEquals( 0, idGenerator.getDefragCount() );
            assertEquals( 23, idGenerator.nextId() );

            when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( 4L, 6L ); // gap-free
            when( state.lastLogIndexBeforeWeBecameLeader() ).thenReturn( 5L );
            idReusabilityCondition.onLeaderSwitch( new LeaderInfo( myself, 1 ) );

            assertEquals( 24, idGenerator.nextId() );
            idGenerator.freeId( 11 );
            assertEquals( 25, idGenerator.nextId() );
            idGenerator.freeId( 6 );
            assertEquals( 6, idGenerator.nextId() );
        }
    }

    private IdReusabilityCondition getIdReusabilityCondition()
    {
        return new IdReusabilityCondition( commandIndexTracker, raftMachine, myself );
    }

    private Set<Long> collectGeneratedIds( ReplicatedIdGenerator idGenerator, long expectedIds )
    {
        Set<Long> idsGenerated = new HashSet<>();

        long nextId;
        for ( int i = 0; i < expectedIds; i++ )
        {
            nextId = idGenerator.nextId();
            assertThat( nextId, greaterThanOrEqualTo( 0L ) );
            idsGenerated.add( nextId );
        }

        assertThrows( NoMoreIds.class, idGenerator::nextId, "Too many ids produced, expected " + expectedIds );

        return idsGenerated;
    }

    private ReplicatedIdRangeAcquirer simpleRangeAcquirer( IdType idType, long start, int length )
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        //noinspection unchecked
        when( rangeAcquirer.acquireIds( idType ) ).thenReturn( allocation( start, length, -1 ) ).thenThrow( NoMoreIds.class );
        return rangeAcquirer;
    }

    private static class NoMoreIds extends RuntimeException
    {
    }

    private IdAllocation allocation( long start, int length, int highestIdInUse )
    {
        return new IdAllocation( new IdRange( new long[0], start, length ), highestIdInUse, 0 );
    }

    private ReplicatedIdRangeAcquirer stubAcquirer()
    {
        final ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        when( rangeAcquirer.acquireIds( IdType.NODE ) )
                .thenReturn( allocation( 0, 1024, -1 ) )
                .thenReturn( allocation( 1024, 1024, 1023 ) )
                .thenReturn( allocation( 2048, 1024, 2047 ) )
                .thenReturn( allocation( 3072, 1024, 3071 ) )
                .thenReturn( allocation( 4096, 1024, 4095 ) )
                .thenReturn( allocation( 5120, 1024, 5119 ) )
                .thenReturn( allocation( 6144, 1024, 6143 ) )
                .thenReturn( allocation( 7168, 1024, 7167 ) )
                .thenReturn( allocation( 8192, 1024, 8191 ) )
                .thenReturn( allocation( 9216, 1024, 9215 ) )
                .thenReturn( allocation( -1, 0, 9216 + 1024 ) );
        return rangeAcquirer;
    }

    private ReplicatedIdGenerator getReplicatedIdGenerator( int grabSize, long l, ReplicatedIdRangeAcquirer replicatedIdRangeAcquirer )
    {
        return new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> l, replicatedIdRangeAcquirer, logProvider, grabSize, true, panicker );
    }
}
