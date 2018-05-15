%{!?dist: %define dist .el7.cern}
%define debug_package %{nil}

Name:           cert-anomaly-ids
Version:        1.0
Release:        1%{?dist}


Summary:        Anomaly-based Intrusion Detection System
Group:          Security/Tools
License:        GPLv2+
URL:            https://gitlab.cern.ch/ComputerSecurity/cert-anomaly-ids
Vendor:         CERN, http://cern.ch/linux
BuildRoot:      %{_tmppath}/%{name}-%{version}-buildroot
BuildRequires:  java-1.8.0-openjdk
Requires:       java-1.8.0-openjdk
BuildArch:      noarch

Source:         %{name}-assembly-%{version}.jar
Source1:        ../../install.sh

%description
Anomaly-based IDS designed on top of Apache Spark for analyzing big amounts of logs stored in Big Data systems like HDFS.

%prep
%setup -q -cT

%build

%install

rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/opt/cert-anomaly-ids/

install -m644 %{SOURCE0} $RPM_BUILD_ROOT/opt/%{name}/%{name}-%{version}.jar
sh %{SOURCE1} %{name} %{version}

%clean
rm -rf ${RPM_BUILD_ROOT}

%files

%defattr(-,root,root)
%attr(644, root, root) /opt/cert-anomaly-ids/%{name}-%{version}.jar

%changelog