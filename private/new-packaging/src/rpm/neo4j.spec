Name: ${PACKAGE_NAME}
Provides: neo4j
Version: ${VERSION}
Release: ${RELEASE}%{?dist}
Summary: Neo4j server is a database that stores data as graphs rather than tables.

License: ${LICENSE}
URL: http://neo4j.org/
#Source: https://github.com/neo4j/neo4j/archive/%{version}.tar.gz

PreReq: dialog
Requires: java-1.8.0-headless, cypher-shell

BuildArch: noarch

%define neo4jhome %{_localstatedir}/lib/neo4j

%description

Neo4j is a highly scalable, native graph database purpose-built to
leverage not only data but also its relationships.


%prep
%build
%clean

# See https://fedoraproject.org/wiki/Packaging:Scriptlets?rd=Packaging:ScriptletSnippets
# The are ordered roughly in the order they will be executed but see manual anyway.

%pretrans

%pre

# The user must accept the license agreement to install Enterprise.
if [ "$PACKAGE_NAME" = "neo4j-enterprise" ]; then
    if [ "$NEO4J_ACCEPT_LICENSE_AGREEMENT" != "yes" ]; then
        DIALOG_TTY=1 dialog --stderr --title "Neo4j Enterprise License (PgUp/PgDn to Scroll)" --scrolltext --msgbox "\

NEO4J, INC. PRE-RELEASE AGREEMENT FOR NEO4J SOFTWARE IMPORTANT -
CAREFULLY READ ALL THE TERMS AND CONDITIONS OF THIS NEO4J PRE-RELEASE
AGREEMENT FOR NEO4J SOFTWARE (THIS \"AGREEMENT\"). BY CLICKING \"I
ACCEPT,\" \"CREATE\", OR PROCEEDING WITH THE INSTALLATION OF THE NEO4J
ENTERPRISE SOFTWARE (\"SOFTWARE\"), OR USING THE SOFTWARE YOU AS AN
AUTHORIZED REPRESENTATIVE OF YOUR COMPANY ON WHOSE BEHALF YOU INSTALL
AND/OR USE THE SOFTWARE (\"LICENSEE\") ARE INDICATING THAT YOU HAVE
READ, UNDERSTAND AND ACCEPT THIS AGREEMENT WITH NEO4J, INC. (\"NEO4J\"),
AND THAT YOU AGREE TO BE BOUND BY ITS TERMS. IF YOU DO NOT AGREE WITH
ALL OF THE TERMS OF THIS AGREEMENT, DO NOT INSTALL, COPY OR OTHERWISE
USE THE SOFTWARE. THE EFFECTIVE DATE OF THIS AGREEMENT SHALL BE THE
DATE THAT LICENSEE ACCEPTS THIS AGREEMENT.


1. Software Pre-release.

(a) License Grant. Subject to Licensee's compliance with the terms and
conditions of this Agreement, Neo4j hereby grants Licensee a limited,
personal, revocable, non-transferable, non-sublicensable,
non-exclusive license to use the Software on one or more Java Virtual
Machines (each an \"Instance\") to develop software applications (each,
an \"Application\"), all solely internally and solely for evaluation
purposes as necessary to determine the feasibility of using the
Software during a \"Pre-release Period\" commencing on the Effective
Date and extending until the earlier to occur of: (x) the thirty (30)
day anniversary date of the Effective Date and (y) the date when the
Software is first generally commercially available or \"GA\". The
Pre-release Period may be extended by Neo4j in writing (an email from
Neo4j management or sales representative will suffice). The Software
will be made available to Licensee under this Agreement in object code
only; no source code is provided to Licensee under this
Agreement. Without limiting any restrictions on Licensee's use of the
Software as set forth in Section 1(c) (Restrictions) below and
elsewhere in this Agreement, Licensee is expressly prohibited from
distributing any copy of the Software (whether in connection with an
Application-based product or service or otherwise) to any third party.

(b) Terms. Notwithstanding the fact that Licensee may already have
obtained a copy of the Software from Neo4j prior to the Effective Date
for Licensee's use under separate software license terms, to the
extent Licensee uses the Software pursuant to the terms of this
Agreement (as evidenced by Licensee entering into this Agreement),
Licensee's use of the Software is solely and exclusively governed by
the terms of this Agreement.  Licensee's use of the Software is also
subject to the terms of the Neo4j User Experience Improvement Program
set forth at https://neo4j.com/legal/neo4j-ux-improvement-program/.

(c) Restrictions. Licensee may not, and will not permit or induce any
third party to: (i) decompile, reverse engineer, disassemble or
otherwise attempt to reconstruct or discover the source code,
underlying ideas or algorithms of any components of the Software; (ii)
alter, modify, translate, adapt in any way, or prepare any derivative
work based upon the Software; (iii) rent, lease, network, loan,
pledge, encumber, sublicense, sell, distribute, disclose, assign or
otherwise transfer the Software or any copy thereof; (iv) use the
Software in commercial timesharing, rental or other sharing
arrangements; or (v) remove any proprietary notices from the Software
or any related documentation or other materials furnished or made
available hereunder. In addition, Licensee agrees to comply with all
applicable local, state, national, and international laws, rules and
regulations applicable to Licensee's use of the Software.

(d) Proprietary Rights. Neo4j or its licensors retain all right, title
and interest in and to the Software and related documentation and
materials, including, without limitation, all patent, copyright,
trademark, and trade secret rights, embodied in, or otherwise
applicable to the Software, whether such rights are registered or
unregistered, and wherever in the world those rights may
exist. Licensee shall not commit any act or omission, or permit or
induce any third party to commit any act or omission inconsistent with
Neo4j's or its licensors' rights, title and interest in and to the
Software and the intellectual property rights embodied therein or
applicable thereto. All materials embodied in, or comprising the
Software, including, but not limited to, graphics, user and visual
interfaces, images, code, applications, and text, as well as the
design, structure, selection, coordination, expression, \"look and
feel\", and arrangement of the Software and its content, and the
trademarks, service marks, proprietary logos and other distinctive
brand features found in the Software (\"Neo4j Marks\"), are all owned by
Neo4j or its licensors; Licensee is expressly prohibited from using
the Neo4j Marks. Title to the Software shall not pass from Neo4j to
Licensee, and the Software and all copies thereof shall at all times
remain the sole and exclusive property of Neo4j. Licensee's embedding
or integration of the Software into an Application is not considered a
derivative work. There are no implied rights or licenses in this
Agreement.  All rights are expressly reserved by Neo4j.

(e) Third Party Software. Neo4j may in its sole discretion, make
available third party software (\"Third Party Software\") embedded in,
or otherwise provided with, the Software. Third Party Software is
expressly excluded from the defined term \"Software\" as used throughout
this Agreement. Licensee's use of the Third Party Software is subject
to the applicable third party license terms which can be viewed at
www.neotechnology.com/thirdpartylicenses, and such Third Party
Software is not licensed to Licensee under the terms of this
Agreement. If Licensee does not agree to abide by the applicable
license terms for the Third Party Software, then Licensee may not
access or use the Software or the Third Party Software. Licensee is
solely and exclusively responsible for determining if Licensee is
permitted to use the Third Party Software in connection with any
Application and Licensee should address any questions in this regard
directly to the relevant Third Party Software licensor. Neo4j makes no
representation or warranty that Licensee is entitled to use the Third
Party Software in connection with any Application.


2. Term & Termination.

(a) Term. Subject to termination as set forth in this Section, the
term of this Agreement will commence on the Effective Date and will
continue until the end of the Pre-release Period, unless extended in
writing by an authorized Neo4j representative (e-mail will suffice for
such extension).

(b) Termination. Neo4j may terminate this Agreement immediately with
or without notice if the Licensee breaches its obligations under this
Agreement. Licensee may terminate this Agreement immediately by
ceasing use of the Software.

(c) Effects of Termination. Upon the termination of this Agreement for
any reason, the licenses granted under this Agreement shall
immediately terminate and unless Licensee and Neo4j have entered into
a subsequent commercial written license agreement governing the
Software, Licensee shall uninstall the Software. Within ten (10)
calendar days of such termination, each party shall destroy or return
all confidential and/or proprietary information of the other party in
its possession, and will not make or retain any copies of such
information in any form, except that the receiving party may retain
one (1) archival copy of such information solely for purposes of
ensuring compliance with this Agreement. If Licensee has not
un-installed the Software within five (5) days of Neo4j's written
notice to Licensee of such failure: (i) Neo4j shall have the right to
invoice Licensee at list price for the number of Instances being used
for an annual period; (ii) Licensee's use will be subject to the terms
and conditions of Neo4j's then-current standard Software License and
Services Agreement terms; and (iii) Licensee agrees to pay such fees
in accordance with the terms of such Software License and Services
Agreement.  Notwithstanding the foregoing, the following terms shall
survive the termination of this Agreement, together with any other
terms which by their nature are intended to survive such termination:
Section 1(c) (Restrictions), 1(d) (Proprietary Rights), 1(e) (Third
Party Software), 2(c) (Effects of Termination), 3(a)
(\"Confidentiality\"), 3(b) (Feedback), 4 (Disclaimer of Warranties), 5
(Indemnification), 6 (Limitation of Liability) and 9 (General).


3. Confidentiality & Feedback.

(a) Confidentiality. \"Confidential Information\" means any proprietary
information received by the other party during, or prior to entering
into, this Agreement that a party should know is confidential or
proprietary based on the circumstances surrounding the disclosure
including, without limitation, the Software and any non-public
technical and business information. Confidential Information does not
include information that (a) is or becomes generally known to the
public through no fault of or breach of this Agreement by the
receiving party; (b) is rightfully known by the receiving party at the
time of disclosure without an obligation of confidentiality; (c) is
independently developed by the receiving party without use of the
disclosing party's Confidential Information; or (d) the receiving
party rightfully obtains from a third party without restriction on use
or disclosure. Licensee and Neo4j will maintain the confidentiality of
Confidential Information. The receiving party of any Confidential
Information of the other party agrees not to use such Confidential
Information for any purpose except as necessary to fulfill its
obligations and exercise its rights under this Agreement. The
receiving party shall protect the secrecy of and prevent disclosure
and unauthorized use of the disclosing party's Confidential
Information using the same degree of care that it takes to protect its
own confidential information and in no event shall use less than
reasonable care. The receiving party may disclose the Confidential
Information of the disclosing party if required by judicial or
administrative process, provided that the receiving party first
provides to the disclosing party prompt notice of such required
disclosure to enable the disclosing party to seek a protective
order. Upon termination or expiration of this Agreement, the receiving
party will, at the disclosing party's option, promptly return or
destroy (and provide written certification of such destruction) the
disclosing party's Confidential Information.

(b) Feedback. To the extent Licensee sends or transmits any
communications, comments, questions, suggestions, or related materials
to Neo4j, whether by letter, e-mail, telephone, or otherwise
(\"Feedback\") suggesting or recommending changes to the Software,
including, without limitation, new features or functionality relating
thereto, Licensee hereby grants Neo4j a perpetual, irrevocable,
non-exclusive, royalty-free, fully-paid-up, fully-transferable,
worldwide license (with rights to sublicense through multiple tiers of
sublicensees) under Licensee's and its licensors' intellectual
property rights to reproduce, prepare derivative works of, distribute,
perform, display, and otherwise fully use, practice and exploit such
Feedback for any purpose whatsoever, including but not limited to,
developing, manufacturing, having manufactured, licensing, marketing,
and selling, directly or indirectly, products and services using such
Feedback. Licensee agrees and understands that Neo4j is not obligated
to use, display, reproduce, or distribute any such ideas, know-how,
concepts, or techniques contained in the Feedback, and Licensee has no
right to compel such use, display, reproduction, or distribution.


4. Limited Development Support and Disclaimer of Warranties. Neo4j may
elect in its sole discretion to provide You with limited development
support on the use of the Software during Neo4j's standard business
hours. This is alpha, beta or release candidate pre-release, time
limited Software.  The Software is meant for evaluation purposes
only. The Software should not be used in a production or commercial
operating environment or with important data. Before using the
Software Licensee should back up all of Licensee's data and regularly
back up data while using the Software. I) THE SOFTWARE IS PROVIDED TO
LICENSEE ON AN \"AS IS\" BASIS, WITH ANY AND ALL FAULTS, AND WITHOUT ANY
WARRANTY OF ANY KIND; AND (II) NEO4J EXPRESSLY DISCLAIMS ALL
REPRESENTATIONS, WARRANTIES AND CONDITIONS WHETHER EXPRESS, IMPLIED,
STATUTORY, OR OTHERWISE, INCLUDING WITHOUT LIMITATION, THE IMPLIED
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE,
SATISFACTORY QUALITY, AND NON-INFRINGEMENT OF THIRD PARTY RIGHTS.
NEO4J DOES NOT WARRANT THAT THE SOFTWARE WILL MEET LICENSEE'S
REQUIREMENTS, OR THAT THE OPERATION OF THE SOFTWARE WILL BE
UNINTERRUPTED OR ERROR-FREE, OR THAT DEFECTS IN THE SOFTWARE WILL BE
CORRECTED. LICENSEE EXPRESSLY ACKNOWLEDGES AND AGREES THAT THE USE OF
THE SOFTWARE AND ALL RESULTS OF SUCH USE IS SOLELY AT LICENSEE'S OWN
RISK. NO ORAL OR WRITTEN INFORMATION OR ADVICE GIVEN BY NEO4J OR ITS
AUTHORIZED REPRESENTATIVES SHALL CREATE A WARRANTY OR IN ANY WAY
INCREASE THE SCOPE OF ANY WARRANTY. SOME JURISDICTIONS MAY NOT ALLOW
THE EXCLUSION AND/OR LIMITATION OF IMPLIED WARRANTIES OR CONDITIONS,
OR ALLOW LIMITATIONS ON HOW LONG AN IMPLIED WARRANTY LASTS, SO THE
ABOVE LIMITATIONS OR EXCLUSIONS MAY NOT APPLY TO LICENSEE. IN SUCH
EVENT, NEO4J'S WARRANTIES AND CONDITIONS WITH RESPECT TO THE SOFTWARE
AND SERVICES WILL BE LIMITED TO THE GREATEST EXTENT PERMITTED BY
APPLICABLE LAW IN SUCH JURISDICTION.


5.Indemnification.

(a) Indemnification. Licensee hereby agrees to indemnify, defend and
hold harmless Neo4j and its parents, affiliates, subsidiaries,
licensors, and third party service providers, and its and their
respective officers, directors, employees, agents, representatives,
and contractors (each, a \"Neo4j Party\"), from and against any and all
liability and costs (including, without limitation, attorneys' fees
and costs) incurred by any Neo4j Party in connection with any actual
or alleged claim arising out of, or relating to: (i) Licensee's breach
of this Agreement, any license applicable to the Third Party Software,
or any applicable law, rule or regulation and (ii) Licensee's gross
negligence, fraudulent misrepresentation or willful misconduct.

(b)Procedure. Counsel Licensee selects for the defense or settlement
of a claim must be consented to by Neo4j prior to counsel being
engaged to represent any Neo4j Party. Licensee and Licensee's counsel
will cooperate as fully as reasonably required, and provide such
information as reasonably requested, by Neo4j in the defense or
settlement of any claim. Neo4j reserves the right, at its own expense,
to assume the exclusive defense or settlement, and control of any
matter otherwise subject to indemnification by Licensee. Licensee
shall not in any event, consent to any judgment, settlement,
attachment, or lien, or any other act adverse to the interests of any
Neo4j Party without the prior written consent of each relevant Neo4j
Party.


6. Limitation of Liability.

(a) Consequential Damages Waiver. UNDER NO CIRCUMSTANCES, SHALL ANY
NEO4J PARTY BE LIABLE TO LICENSEE OR ANY THIRD PARTY FOR ANY INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE, RELIANCE, OR CONSEQUENTIAL
DAMAGES, (INCLUDING, WITHOUT LIMITATION, DAMAGES FOR LOSS OF BUSINESS
PROFITS, BUSINESS INTERRUPTION, LOSS OF BUSINESS INFORMATION AND THE
LIKE) ARISING OUT OF OR RELATING TO THE USE AND/OR INABILITY TO USE
THE SOFTWARE, REGARDLESS OF THE LEGAL THEORY UPON WHICH ANY CLAIM FOR
SUCH DAMAGES IS BASED AND EVEN IF A NEO4J PARTY HAS BEEN ADVISED OF
THE POSSIBILITY OF SUCH DAMAGES.

(b) Limitation of Damages. WITHOUT LIMITING THE FOREGOING, IN NO EVENT
SHALL THE NEO4J PARTIES' TOTAL CUMULATIVE LIABILITY TO LICENSEE OR ANY
THIRD PARTY FOR ALL DAMAGES, LOSSES AND CAUSES OF ACTION (WHETHER IN
CONTRACT, TORT, INCLUDING NEGLIGENCE AND STRICT LIABILITY, OR
OTHERWISE) EXCEED THE AMOUNT OF TWO HUNDRED AND FIFTY DOLLARS
($250.00).

(c) Liability for Third Party Software. IF ANY LIABILITY ATTACHES TO
ANY NEO4J PARTY IN RESPECT OF THIRD PARTY SOFTWARE, SUCH LIABILITY
WILL BE LIMITED BY THIS SECTION 6 AND THE DISCLAIMER OF WARRANTIES SET
FORTH IN SECTION 4 (DISCLAIMER OF WARRANTIES) ABOVE.

(d) Failure of Essential Purpose. THE PARTIES AGREE THAT THESE
LIMITATIONS SHALL APPLY EVEN IF THIS AGREEMENT OR ANY LIMITED REMEDY
SPECIFIED HEREIN IS FOUND TO HAVE FAILED OF ITS ESSENTIAL
PURPOSE. SOME JURISDICTIONS MAY NOT ALLOW THE EXCLUSION OR LIMITATION
OF INCIDENTAL, SPECIAL, CONSEQUENTIAL, OR OTHER DAMAGES, SO THE ABOVE
LIMITATIONS OR EXCLUSIONS MAY NOT APPLY TO LICENSEE. IN SUCH EVENT,
THE LIABILITY OF THE NEO4J PARTIES FOR SUCH DAMAGES WITH RESPECT TO
THE SOFTWARE AND CONSULTING SERVICES WILL BE LIMITED TO THE GREATEST
EXTENT PERMITTED BY APPLICABLE LAW IN SUCH JURISDICTION. The sections
of this Agreement that address indemnification, limitation of
liability and the disclaimer of warranties allocate the risk between
the parties. This allocation of risk is an essential element of the
basis of the bargain between the parties.  7. Government Rights. The
Software licensed to Licensee under this Agreement is \"commercial
computer software\" as that term is described in DFAR
252.227-7014(a)(1). If acquired by or on behalf of a civilian agency,
the U.S.  Government acquires this commercial computer software and/or
commercial computer software documentation subject to the terms of
this Agreement as specified in 48 C.F.R. 12.212 (Computer Software)
and 12.11 (Technical Data) of the Federal Acquisition Regulations
(\"FAR\") and its successors. If acquired by or on behalf of any agency
within the Department of Defense (\"DOD\"), the U.S.  Government
acquires this commercial computer software and/or commercial computer
software documentation subject to the terms of this Agreement as
specified in 48 C.F.R. 227.7202 of the DOD FAR Supplement and its
successors.


8. Export. Licensee acknowledges that the laws and regulations of the
United States of America and foreign jurisdictions may restrict the
export and re-export of certain commodities and technical data of
United States of America origin, including the Software. Licensee
agrees that it will comply with all export control laws and
regulations.


9. General. This Agreement will be construed and enforced in all
respects in accordance with the laws of the state of California,
without reference to its choice of law rules. Except as set forth
below in this Section, the federal and state courts seated in San
Francisco, San Mateo and Santa Clara Counties, California, will have
sole and exclusive jurisdiction for all purposes in connection with
any action or proceeding that arises from, or relates to, this
Agreement, and each party hereby irrevocably waives any objection to
such exclusive jurisdiction. Notwithstanding anything in this
Agreement to the contrary, Neo4j may seek injunctive or other
equitable relief in any court of competent jurisdiction to protect any
actual or threatened misappropriation or infringement of its
intellectual property rights or those of its licensors, and Licensee
hereby submits to the exclusive jurisdiction of such courts and waives
any objection thereto on the basis of improper venue, inconvenience of
the forum or any other grounds. Licensee agrees that any breach of the
license restrictions or other infringement or misappropriation of the
intellectual property rights of Neo4j or its licensors will result in
immediate and irreparable damage to Neo4j for which there is no
adequate remedy at law. The United Nations Convention on Contracts for
the International Sale of Goods in its entirety is expressly excluded
from this Agreement, including, without limitation, application to the
Software provided hereunder. Furthermore, this Agreement (including
without limitation, the Software provided hereunder) will not be
governed or interpreted in any way by referring to any law based on
the Uniform Computer Information Transactions Act (UCITA) or any other
act derived from or related to UCITA. Licensee consents to receive
communications from Neo4j electronically, including by
e-mail. Licensee agrees that all agreements, notices, disclosures, and
other communications that Neo4j provides to Licensee electronically
satisfy any legal requirement that such communications be in writing,
to the extent permitted by applicable law. Licensee shall not assign
this Agreement or transfer any of its rights hereunder, or delegate
the performance of any of its duties or obligations arising under this
Agreement, whether by merger, acquisition, sale of assets, operation
of law, or otherwise, without the prior written consent of Neo4j. Any
purported assignment in violation of the preceding sentence is null
and void. Subject to the foregoing, this Agreement shall be binding
upon, and inure to the benefit of, the successors and assigns of the
parties thereto. Except as otherwise specified in this Agreement, this
Agreement may be amended or supplemented only by a writing that refers
explicitly to this Agreement and that is signed on behalf of both
parties. No waiver will be implied from conduct or failure to enforce
rights.  No waiver will be effective unless in a writing signed on
behalf of the party against whom the waiver is asserted. If any term
of this Agreement is found invalid or unenforceable that term will be
enforced to the maximum extent permitted by law and the remainder of
this Agreement will remain in full force.  The parties are independent
contractors and nothing contained herein shall be construed as
creating an agency, partnership, or other form of joint enterprise
between the parties. This Agreement represents the entire agreement
between the parties relating to its subject matter and supersedes all
prior and/or contemporaneous representations, discussions,
negotiations and agreements, whether written or oral, except to the
extent Neo4j makes any software or other products and services
available to Licensee under separate written terms. This Agreement
shall not be interpreted or construed to confer any rights or remedies
on any third parties, except that each Neo4j Party shall be a third
party beneficiary hereunder and accordingly, shall be entitled to
directly enforce and rely upon any provision of this Agreement that
confers a right or remedy in favor of it." 78 78

        if ! DIALOG_TTY=1 dialog --title "NEO4J LICENSE AGREEMENT" --defaultno --yesno "\

Do you accept the terms of the license agreement?" 10 60; then
            exit 1
        fi
    fi
fi

# Create neo4j user if it doesn't exist.
if ! id neo4j > /dev/null 2>&1 ; then
  useradd --system --user-group --home %{neo4jhome} --shell /bin/bash neo4j
else
  # Make sure a neo4j group exists in case user is upgrading
  # from older version where no such group was created
  groupadd --system --force neo4j
  # Make sure neo4j user's primary group is neo4j
  usermod --gid neo4j neo4j > /dev/null 2>&1
fi

if [ $1 -gt 1 ]; then
  # Upgrading
  # Remember if neo4j is running
  if [ -e "/run/systemd/system" ]; then
    # SystemD is the init-system
    if systemctl is-active --quiet neo4j > /dev/null 2>&1 ; then
      mkdir -p %{_localstatedir}/lib/rpm-state/neo4j
      touch %{_localstatedir}/lib/rpm-state/neo4j/running
      systemctl stop neo4j > /dev/null 2>&1 || :
    fi
  else
    # SysVInit must be the init-system
    if service neo4j status > /dev/null 2>&1 ; then
      mkdir -p %{_localstatedir}/lib/rpm-state/neo4j
      touch %{_localstatedir}/lib/rpm-state/neo4j/running
      service neo4j stop > /dev/null 2>&1 || :
    fi
  fi
fi


%post

# Pre uninstalling (includes upgrades)
%preun

if [ $1 -eq 0 ]; then
  # Uninstalling
  if [ -e "/run/systemd/system" ]; then
    systemctl stop neo4j > /dev/null 2>&1 || :
    systemctl disable --quiet neo4j > /dev/null 2>&1 || :
  else
    service neo4j stop > /dev/null 2>&1 || :
    chkconfig --del neo4j > /dev/null 2>&1 || :
  fi
fi


%postun

%posttrans

# Restore neo4j if it was running before upgrade
if [ -e %{_localstatedir}/lib/rpm-state/neo4j/running ]; then
  rm %{_localstatedir}/lib/rpm-state/neo4j/running
  if [ -e "/run/systemd/system" ]; then
    systemctl daemon-reload > /dev/null 2>&1 || :
    systemctl start neo4j  > /dev/null 2>&1 || :
  else
    service neo4j start > /dev/null 2>&1 || :
  fi
fi


%install
mkdir -p %{buildroot}/%{_bindir}
mkdir -p %{buildroot}/%{_datadir}/neo4j/lib
mkdir -p %{buildroot}/%{_datadir}/neo4j/bin/tools
mkdir -p %{buildroot}/%{_datadir}/doc/neo4j
mkdir -p %{buildroot}/%{neo4jhome}/plugins
mkdir -p %{buildroot}/%{neo4jhome}/data/databases
mkdir -p %{buildroot}/%{neo4jhome}/import
mkdir -p %{buildroot}/%{_sysconfdir}/neo4j
mkdir -p %{buildroot}/%{_localstatedir}/log/neo4j
mkdir -p %{buildroot}/%{_localstatedir}/run/neo4j
mkdir -p %{buildroot}/lib/systemd/system
mkdir -p %{buildroot}/%{_mandir}/man1
mkdir -p %{buildroot}/%{_sysconfdir}/default
mkdir -p %{buildroot}/%{_sysconfdir}/init.d

cd %{name}-%{version}

install neo4j.service %{buildroot}/lib/systemd/system/neo4j.service
install -m 0644 neo4j.default %{buildroot}/%{_sysconfdir}/default/neo4j
install -m 0755 neo4j.init %{buildroot}/%{_sysconfdir}/init.d/neo4j

install -m 0644 server/conf/* %{buildroot}/%{_sysconfdir}/neo4j

install -m 0755 server/scripts/* %{buildroot}/%{_bindir}

install -m 0755 server/lib/* %{buildroot}/%{_datadir}/neo4j/lib

cp -r server/bin/* %{buildroot}/%{_datadir}/neo4j/bin
chmod -R 0755 %{buildroot}/%{_datadir}/neo4j/bin

install -m 0644 server/README.txt %{buildroot}/%{_datadir}/doc/neo4j/README.txt
install -m 0644 server/UPGRADE.txt %{buildroot}/%{_datadir}/doc/neo4j/UPGRADE.txt
install -m 0644 server/LICENSES.txt %{buildroot}/%{_datadir}/doc/neo4j/LICENSES.txt

install -m 0644 manpages/* %{buildroot}/%{_mandir}/man1

%files
%defattr(-,root,root)
# Needed to make sure empty directories get created
%dir %{neo4jhome}/plugins
%dir %{neo4jhome}/import
%dir %{neo4jhome}/data/databases
%attr(-,neo4j,neo4j) %dir %{_localstatedir}/run/neo4j

%{_datadir}/neo4j
%{_bindir}/*
%attr(-,neo4j,neo4j) %{neo4jhome}
%attr(-,neo4j,neo4j) %dir %{_localstatedir}/log/neo4j
/lib/systemd/system/neo4j.service
%{_sysconfdir}/init.d/neo4j

%config(noreplace) %{_sysconfdir}/default/neo4j
%attr(-,neo4j,neo4j) %config(noreplace) %{_sysconfdir}/neo4j

%doc %{_mandir}/man1/*
%doc %{_datadir}/doc/neo4j/README.txt
%doc %{_datadir}/doc/neo4j/UPGRADE.txt

%license %{_datadir}/doc/neo4j/LICENSES.txt
