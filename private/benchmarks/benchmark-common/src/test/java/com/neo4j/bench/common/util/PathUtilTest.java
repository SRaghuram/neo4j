/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class PathUtilTest
{
    private static final String FILENAME = "123456";
    private static final String FILENAME_HASH = DigestUtils.md5Hex( FILENAME );

    @Test
    public void shouldAddMd5SumToFilesBelowLimit()
    {
        String expectedOutput = FILENAME + "_" + FILENAME_HASH;
        String output = PathUtil.withMaxLength( expectedOutput.length() ).limitLength( FILENAME );

        assertThat( output, equalTo( expectedOutput ) );
    }

    @Test
    public void shouldTruncateTooLongFilenamesAndAddMd5Sum()
    {
        String expectedOutput = "12345" + "_" + FILENAME_HASH;

        String output = PathUtil.withMaxLength( expectedOutput.length() ).limitLength( FILENAME );

        assertThat( output, equalTo( expectedOutput ) );
    }

    @Test
    public void shouldAddMd5SumToFilenamesWithExtensionBelowLengthLimit()
    {
        String extension = ".gz";
        String expectedOutput = FILENAME + "_" + FILENAME_HASH + extension;

        String output = PathUtil.withMaxLength( expectedOutput.length() ).limitLength( FILENAME, extension );

        assertThat( output, equalTo( expectedOutput ) );
    }

    @Test
    public void shouldTruncateTooLongFilenamesWithExtensionAndAddMd5Sum()
    {
        String extension = ".gz";
        String expectedOutput = "123" + "_" + FILENAME_HASH + extension;

        String output = PathUtil.withMaxLength( expectedOutput.length() ).limitLength( FILENAME, extension );

        assertThat( output, equalTo( expectedOutput ) );
    }

    @Test
    public void shouldAddMd5SumToFilenamesWithPrefixAndExtensionBelowLengthLimit()
    {
        String prefix = "A-";
        String extension = ".gz";
        String expectedOutput = prefix + FILENAME + "_" + FILENAME_HASH + extension;

        String output = PathUtil.withMaxLength( expectedOutput.length() ).limitLength( prefix, FILENAME, extension );

        assertThat( output, equalTo( expectedOutput ) );
    }

    @Test
    public void shouldTruncateTooLongFilenamesWithPrefixAndExtensionAndAddMd5Sum()
    {
        String prefix = "A-";
        String extension = ".gz";
        String expectedOutput = prefix + "1" + "_" + FILENAME_HASH + extension;

        String output = PathUtil.withMaxLength( expectedOutput.length() ).limitLength( prefix, FILENAME, extension );

        assertThat( output, equalTo( expectedOutput ) );
    }

    @Test
    public void shouldFailWhenLimitIsTooSmall()
    {
        String prefix = "A";
        String filename = "B";
        String extension = "C";
        PathUtil pathUtil = PathUtil.withMaxLength( FILENAME_HASH.length() + 3 ); // it requires one more for the separator

        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> pathUtil.limitLength( prefix, filename, extension ) );
    }
}
