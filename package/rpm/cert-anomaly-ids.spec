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
Source1:        cert-anomaly-ids_env.sh
Source2:        cert-anomaly-ids

%description
Anomaly-based IDS designed on top of Apache Spark for analyzing big amounts of logs stored in Big Data systems like HDFS.

%prep
%setup -q -cT

%build

%install

rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/opt/cert-anomaly-ids/
mkdir -p $RPM_BUILD_ROOT/opt/cert-anomaly-ids/bin/
mkdir -p $RPM_BUILD_ROOT/%{_sysconfdir}/profile.d/

install -m644 %{SOURCE0} $RPM_BUILD_ROOT/opt/%{name}/%{name}-%{version}.jar
ln -s /opt/%{name}/%{name}-%{version}.jar $RPM_BUILD_ROOT/opt/%{name}/%{name}.jar
install -m644 %{SOURCE1} $RPM_BUILD_ROOT/%{_sysconfdir}/profile.d/cert-anomaly-ids_env.sh
install -m644 %{SOURCE2} $RPM_BUILD_ROOT/opt/%{name}/bin/cert-anomaly-ids

%clean
rm -rf ${RPM_BUILD_ROOT}

%files

%defattr(-,root,root)
%attr(644, root, root) /opt/%{name}/%{name}-%{version}.jar
/opt/%{name}/%{name}.jar
%attr(755, root, root) /opt/%{name}/bin/cert-anomaly-ids
%config %{_sysconfdir}/profile.d/cert-anomaly-ids_env.sh

%changelog