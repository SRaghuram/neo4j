/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class SelfSignedCertificatesIT
{

    @Inject
    private TestDirectory testDirectory;

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void createSelfSignedCertificateWithCorrectPermissions() throws Exception
    {
        var certificates = new SelfSignedCertificateFactory();
        certificates.createSelfSignedCertificate( testDirectory.file( "certificate" ), testDirectory.file( "privateKey" ), "localhost" );

        PosixFileAttributes certificateAttributes =
                Files.getFileAttributeView( testDirectory.file( "certificate" ), PosixFileAttributeView.class )
                        .readAttributes();

        assertTrue( certificateAttributes.permissions().contains( PosixFilePermission.OWNER_READ ) );
        assertTrue( certificateAttributes.permissions().contains( PosixFilePermission.OWNER_WRITE ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OWNER_EXECUTE ) );

        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.GROUP_READ ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.GROUP_WRITE ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.GROUP_EXECUTE ) );

        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OTHERS_READ ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OTHERS_WRITE ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OTHERS_EXECUTE ) );

        PosixFileAttributes privateKey =
                Files.getFileAttributeView( testDirectory.file( "privateKey" ), PosixFileAttributeView.class )
                        .readAttributes();

        assertTrue( privateKey.permissions().contains( PosixFilePermission.OWNER_READ ) );
        assertTrue( privateKey.permissions().contains( PosixFilePermission.OWNER_WRITE ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.OWNER_EXECUTE ) );

        assertFalse( privateKey.permissions().contains( PosixFilePermission.GROUP_READ ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.GROUP_WRITE ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.GROUP_EXECUTE ) );

        assertFalse( privateKey.permissions().contains( PosixFilePermission.OTHERS_READ ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.OTHERS_WRITE ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.OTHERS_EXECUTE ) );
    }
}
