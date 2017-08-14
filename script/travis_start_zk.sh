#! /bin/bash

if ! test -e /usr/bin/strace; then
  sudo apt-get install strace
fi

while true; do
  ./zk/bin/zkServer.sh start ./zk/conf/zoo.cfg
  if echo stat |nc localhost 2181 |grep -q Mode; then
    break
  fi
  echo zk did not start properly, retrying...
  lsof -p $(cat /tmp/zookeeper/zookeeper_server.pid)
  strace -f -p $(cat /tmp/zookeeper/zookeeper_server.pid)
  ./zk/bin/zkServer.sh stop
done
