/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.info;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.ExternalSettings;
import org.neo4j.logging.BufferingLog;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.neo4j.kernel.info.JvmChecker.INCOMPATIBLE_JVM_VERSION_WARNING;
import static org.neo4j.kernel.info.JvmChecker.INCOMPATIBLE_JVM_WARNING;
import static org.neo4j.kernel.info.JvmChecker.memorySettingWarning;

class JVMCheckerTest
{
    @Test
    void shouldIssueWarningWhenUsingHotspotServerVmVersion7()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.7.0-b147" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    void shouldNotIssueWarningWhenUsingHotspotServerVmVersion8()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.8.0_45" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), not( containsString( INCOMPATIBLE_JVM_VERSION_WARNING ) ) );
    }

    @Test
    void shouldNotIssueWarningWhenUsingIbmJ9Vm()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "IBM J9 VM", "1.8" ) )
                .checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), not( containsString( INCOMPATIBLE_JVM_VERSION_WARNING ) ) );
    }

    @Test
    void shouldIssueWarningWhenUsingHotspotServerVmVersion7InThe32BitVersion()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) Server VM",
                "1.7.0_25-b15" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    void shouldIssueWarningWhenUsingOpenJDKServerVmVersion7()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "OpenJDK 64-Bit Server VM",
                "1.7.0-b147" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    void shouldIssueWarningWhenUsingOpenJDKClientVmVersion7()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "OpenJDK Client VM",
                "1.7.0-b147" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    void shouldIssueWarningWhenUsingUnsupportedJvm()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "MyOwnJDK 64-Bit Awesome VM",
                "1.7" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( INCOMPATIBLE_JVM_WARNING ) );
    }

    @Test
    void shouldIssueWarningWhenUsingUnsupportedJvmVersion()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.6.42_87" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( INCOMPATIBLE_JVM_VERSION_WARNING ) );
    }

    @Test
    void warnAboutMissingInitialHeapSize()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.8.0_45", singletonList( "-XMx" ), 12, 23 ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( memorySettingWarning( ExternalSettings.initialHeapSize, 12 ) ) );
    }

    @Test
    void warnAboutMissingMaximumHeapSize()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.8.0_45", singletonList( "-XMs" ), 12, 23 ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( memorySettingWarning( ExternalSettings.maxHeapSize, 23 ) ) );
    }

    @Test
    void warnAboutMissingHeapSizes()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM",
                "1.8.0_45" ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), containsString( memorySettingWarning( ExternalSettings.initialHeapSize, 1 ) ) );
        assertThat( bufferingLogger.toString(), containsString( memorySettingWarning( ExternalSettings.maxHeapSize, 2 ) ) );
    }

    @Test
    void doNotWarnAboutMissingHeapSizesWhenOptionsSpecified()
    {
        BufferingLog bufferingLogger = new BufferingLog();

        new JvmChecker( bufferingLogger, new CannedJvmMetadataRepository( "Java HotSpot(TM) 64-Bit Server VM", "1.8.0_45",
                asList( "-xMx", "-xmS" ), 1, 2 ) ).checkJvmCompatibilityAndIssueWarning();

        assertThat( bufferingLogger.toString(), not( containsString( memorySettingWarning( ExternalSettings.initialHeapSize, 1 ) ) ) );
        assertThat( bufferingLogger.toString(), not( containsString( memorySettingWarning( ExternalSettings.maxHeapSize, 2 ) ) ) );
    }
}
