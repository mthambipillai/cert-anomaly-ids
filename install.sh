#!/usr/bin/env bash
mkdir $RPM_BUILD_ROOT/opt/$1/bin
echo "#!/usr/bin/env bash" > $RPM_BUILD_ROOT/opt/$1/bin/$1
echo "spark-submit $RPM_BUILD_ROOT/opt/$1/$1-$2.jar \"\$@\"" >> $RPM_BUILD_ROOT/opt/$1/bin/$1
chmod +x $RPM_BUILD_ROOT/opt/$1/bin/$1
echo "export CERT_ANOMALY_IDS_HOME=$RPM_BUILD_ROOT/opt/$1" > /etc/profile.d/$1_setup.sh
echo "export PATH=\$PATH:\$CERT_ANOMALY_IDS_HOME/bin" >> /etc/profile.d/$1_setup.sh