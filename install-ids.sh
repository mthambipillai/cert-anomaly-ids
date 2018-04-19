sudo tar xf spark-ids.tar.gz -C /usr/local/
printf "\nexport SPARK_IDS_HOME=/usr/local/spark-ids" >> ~/.bashrc
printf "\nexport PATH=\$PATH:\$SPARK_IDS_HOME/bin" >> ~/.bashrc
echo "Spark-IDS successfully installed"