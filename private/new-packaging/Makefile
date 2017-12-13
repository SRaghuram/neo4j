.DEFAULT: all
.PHONY: all clean debian rpm

DISTRIBUTION ?= unstable

VERSION := $(shell bin/extract-version-from-pom ../pom.xml)

TARBALL_C := neo4j-community-$(VERSION)-unix.tar.gz
TARBALL_E := neo4j-enterprise-$(VERSION)-unix.tar.gz
ARTIFACT_C := ../../neo4j/packaging/standalone/target
ARTIFACT_E := ../packaging/standalone/target
TARBALLS := $(ARTIFACT_C)/$(TARBALL_C) $(ARTIFACT_E)/$(TARBALL_E)
COMMUNITY_LICENSE := ../../neo4j/community/LICENSE.txt
COMMUNITY_POM := ../../neo4j/community/pom.xml
CYPHER_SHELL := ../packaging/standalone/src/main/distribution/cypher-shell
OUT := out
SHELL_SCRIPTS := ../packaging/standalone/src/main/distribution/shell-scripts/bin

COMMON_FILES := $(shell find src/common -type f)
RPM_FILES := $(shell find src/rpm -type f)
DEBIAN_FILES := $(shell find src/debian -type f)

DEBS=$(patsubst ${ARTIFACT_C}/%-unix.tar.gz,${OUT}/debian/%,${TARBALLS}) $(patsubst ${ARTIFACT_E}/%-unix.tar.gz,${OUT}/debian/%,${TARBALLS})
RPMS=$(patsubst ${ARTIFACT_C}/%-unix.tar.gz,${OUT}/rpm/%,${TARBALLS}) $(patsubst ${ARTIFACT_E}/%-unix.tar.gz,${OUT}/rpm/%,${TARBALLS})

all: debian rpm

clean:
	rm -rf ${OUT}

debian: ${DEBS}

rpm: ${RPMS}

${OUT}/rpm/%: ${ARTIFACT_C}/%-unix.tar.gz $(COMMON_FILES) $(RPM_FILES)
	rm -rf $@
	bin/build-rpm-package $@ $< ${DISTRIBUTION}
	
${OUT}/rpm/%: ${ARTIFACT_E}/%-unix.tar.gz $(COMMON_FILES) $(RPM_FILES)
	rm -rf $@
	bin/build-rpm-package $@ $< ${DISTRIBUTION}

${OUT}/debian/%: ${ARTIFACT_C}/%-unix.tar.gz $(COMMON_FILES) $(DEBIAN_FILES)
	rm -rf $@
	bin/build-debian-package $@ $< ${DISTRIBUTION}
	
${OUT}/debian/%: ${ARTIFACT_E}/%-unix.tar.gz $(COMMON_FILES) $(DEBIAN_FILES)
	rm -rf $@
	bin/build-debian-package $@ $< ${DISTRIBUTION}

$(TARBALLS):
	cd ../packaging && mvn package -DskipTests -T2C
