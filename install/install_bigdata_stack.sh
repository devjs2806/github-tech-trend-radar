#!/usr/bin/env bash
# Installs Java 8, Hadoop 3.3.6, Spark 3.5.3, Hive 3.1.3, Flume 1.11.0 (Ubuntu 22.04 / WSL)
set -e
BD="$HOME/bigdata"
MIRROR="https://archive.apache.org/dist"
HADOOP_V=3.3.6
SPARK_V=3.5.3
HIVE_V=3.1.3
FLUME_V=1.11.0
PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
say() { echo; echo "================ $* ================"; }
download() {
  if [ -f "$BD/downloads/$2.ok" ]; then echo "   $2 already downloaded"; return; fi
  echo "   downloading $2 ..."
  wget -c -q --show-progress --tries=5 -O "$BD/downloads/$2" "$1"
  touch "$BD/downloads/$2.ok"
}
say "1/9  System packages (sudo password needed)"
sudo apt-get update -y
sudo apt-get install -y openjdk-8-jdk openssh-server openssh-client wget curl python3 python3-pip python3-venv unzip nano
mkdir -p "$BD/downloads"
JAVA_HOME_DIR="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
if [ -d /usr/lib/jvm/java-8-openjdk-amd64 ]; then JAVA_HOME_DIR=/usr/lib/jvm/java-8-openjdk-amd64; fi
if [ -d /usr/lib/jvm/java-8-openjdk-arm64 ]; then JAVA_HOME_DIR=/usr/lib/jvm/java-8-openjdk-arm64; fi
echo "   JAVA_HOME = $JAVA_HOME_DIR"
say "2/9  Password-less SSH to localhost"
mkdir -p ~/.ssh && chmod 700 ~/.ssh
[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa -q
grep -qf ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys 2>/dev/null || cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
grep -q "Host localhost" ~/.ssh/config 2>/dev/null || printf 'Host localhost 0.0.0.0 127.0.0.1\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n' >> ~/.ssh/config
chmod 600 ~/.ssh/config
sudo service ssh start || sudo systemctl start ssh || true
say "3/9  Downloading Hadoop, Spark, Hive, Flume (~1.5 GB)"
download "$MIRROR/hadoop/common/hadoop-$HADOOP_V/hadoop-$HADOOP_V.tar.gz" hadoop.tar.gz
download "$MIRROR/spark/spark-$SPARK_V/spark-$SPARK_V-bin-hadoop3.tgz" spark.tgz
download "$MIRROR/hive/hive-$HIVE_V/apache-hive-$HIVE_V-bin.tar.gz" hive.tar.gz
download "$MIRROR/flume/$FLUME_V/apache-flume-$FLUME_V-bin.tar.gz" flume.tar.gz
say "4/9  Extracting"
extract() {
  if [ -d "$BD/$3" ]; then echo "   $3 already extracted"; return; fi
  tar -xzf "$BD/downloads/$1" -C "$BD"; mv "$BD/$2" "$BD/$3"; echo "   $BD/$3"
}
extract hadoop.tar.gz "hadoop-$HADOOP_V" hadoop
extract spark.tgz "spark-$SPARK_V-bin-hadoop3" spark
extract hive.tar.gz "apache-hive-$HIVE_V-bin" hive
extract flume.tar.gz "apache-flume-$FLUME_V-bin" flume
say "5/9  Environment variables"
cat > "$BD/env.sh" <<EOF
export JAVA_HOME=$JAVA_HOME_DIR
export HADOOP_HOME=$BD/hadoop
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$BD/spark
export HIVE_HOME=$BD/hive
export FLUME_HOME=$BD/flume
export PDSH_RCMD_TYPE=ssh
export PYSPARK_PYTHON=$HOME/radar-venv/bin/python
export PYSPARK_DRIVER_PYTHON=$HOME/radar-venv/bin/python
export PATH=$HOME/radar-venv/bin:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$HIVE_HOME/bin:\$FLUME_HOME/bin:\$PATH
EOF
grep -q "bigdata/env.sh" ~/.bashrc || echo "source $BD/env.sh" >> ~/.bashrc
source "$BD/env.sh"
say "6/9  Configuring Hadoop (HDFS on localhost:9000)"
mkdir -p "$BD/hdfs_data/namenode" "$BD/hdfs_data/datanode" "$BD/hdfs_data/tmp"
cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$BD/hdfs_data/tmp</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$BD/hdfs_data/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$BD/hdfs_data/datanode</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>
EOF
sed -i '/^export JAVA_HOME=/d' "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export JAVA_HOME=$JAVA_HOME_DIR" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
[ -d "$BD/hdfs_data/namenode/current" ] || hdfs namenode -format -force -nonInteractive
say "7/9  Starting HDFS"
start-dfs.sh
sleep 8
hdfs dfsadmin -safemode leave || true
hdfs dfs -mkdir -p /tmp /user/hive/warehouse /github/raw/archive /github/raw/live
hdfs dfs -chmod -R 777 /tmp /user/hive/warehouse
say "8/9  Configuring Hive and Flume"
GUAVA="$(ls "$HADOOP_HOME"/share/hadoop/common/lib/guava-*.jar | head -1)"
rm -f "$HIVE_HOME"/lib/guava-*.jar "$FLUME_HOME"/lib/guava-*.jar
cp "$GUAVA" "$HIVE_HOME/lib/"; cp "$GUAVA" "$FLUME_HOME/lib/"
cp "$HIVE_HOME"/hcatalog/share/hcatalog/hive-hcatalog-core-*.jar "$HIVE_HOME/lib/" 2>/dev/null || true
cat > "$HIVE_HOME/conf/hive-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:derby:;databaseName=$BD/hive_metastore_db;create=true</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.apache.derby.jdbc.EmbeddedDriver</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>
  <property><name>hive.cli.print.header</name><value>true</value></property>
  <property><name>hive.exec.mode.local.auto</name><value>true</value></property>
  <property><name>mapreduce.framework.name</name><value>local</value></property>
</configuration>
EOF
cp "$HIVE_HOME/conf/hive-env.sh.template" "$HIVE_HOME/conf/hive-env.sh" 2>/dev/null || true
grep -q "^export HADOOP_HOME" "$HIVE_HOME/conf/hive-env.sh" || echo "export HADOOP_HOME=$HADOOP_HOME" >> "$HIVE_HOME/conf/hive-env.sh"
[ -d "$BD/hive_metastore_db" ] || (cd "$BD" && schematool -dbType derby -initSchema)
cp "$FLUME_HOME/conf/flume-env.sh.template" "$FLUME_HOME/conf/flume-env.sh" 2>/dev/null || true
grep -q "^export JAVA_HOME" "$FLUME_HOME/conf/flume-env.sh" || echo "export JAVA_HOME=$JAVA_HOME_DIR" >> "$FLUME_HOME/conf/flume-env.sh"
grep -q "^export JAVA_OPTS" "$FLUME_HOME/conf/flume-env.sh" || echo 'export JAVA_OPTS="-Xms256m -Xmx1g"' >> "$FLUME_HOME/conf/flume-env.sh"
say "9/9  Python environment + packages"
[ -d "$HOME/radar-venv" ] || python3 -m venv "$HOME/radar-venv"
"$HOME/radar-venv/bin/pip" install -q --upgrade pip setuptools wheel
"$HOME/radar-venv/bin/pip" install -q requests pandas matplotlib "streamlit>=1.38" "pyspark==$SPARK_V"
echo; echo "=========== INSTALLATION COMPLETE ==========="
java -version 2>&1 | head -1; hadoop version | head -1
jps
echo "Close this window, open Ubuntu again, then:  cd ~/github_trend_radar && bash setup.sh"
