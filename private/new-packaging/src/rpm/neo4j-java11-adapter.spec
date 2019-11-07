Name:    neo4j-java11-adapter
Version: 1
Release: 1
Summary: Adapter for Oracle Java 11 so that Neo4j and cypher-shell can install correctly.
URL: http://neo4j.com/
BuildArch: noarch
License: GPLv3

Provides: jre-11-headless = 11.0.~, jre-11 = 11.0.~
Requires: jre >= 2000:11

%description
Oracle and OpenJDK provide incompatible RPM packages for Java 11, so it is impossible to reliably specify either openjdk or oracle java as a dependency.

This adapter for Oracle Java 11 will stop the package manger from installing OpenJDK 11 as a dependency over an existing Oracle Java 11 installation.

The adapter contains no code, and installs no files.
%build
%install
%files
