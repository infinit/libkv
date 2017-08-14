#! /bin/bash

if ! test -e /usr/bin/strace; then
  sudo apt-get install strace
fi

while true; do
  strace -f -o /tmp/zk.trace ./zk/bin/zkServer.sh start ./zk/conf/zoo.cfg &
  sleep 2
  if echo stat |nc localhost 2181 |grep -q Mode; then
    break
  fi
  echo zk did not start properly, retrying...
  cat /tmp/zk.trace
  ./zk/bin/zkServer.sh stop
done
