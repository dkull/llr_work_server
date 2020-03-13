# llr_work_server
Work server and client for Jean Penné's LLR 64

Allows a server-client architecture to be used for distributing work to LLR64 instances.

This isn't actually meant to be used by third parties, so it's pretty ugly and hacky. But it works and does everything I need it to do:
* distribute work to clients
* allow scheduling of threads for clients based on time
* keep track of done work
* keep track of in-progress work
* fault-tolerant networking (using zerorpc)
* actively read new work on every request (allows eg. sieving while running server)
* pretty stable

Server needs folders to work:
  - in_progress
  - results
  - work
Files:
  - schedule.lst  # can be empty at first
  
* work - contains npg files that need to be worked on
* results - contains results for completed work, used actively to know what is done
* in_progress - keeps track of given out unfinished work

schedule.lst file can be used with server to create schedules for clients, it contains lines:
```
<machine_name>:<threads>:<nth_weekday,>:<hour_start>+<duration>
eg: machine-abc:4:0,1,2,3,4:6+11
```
This causes client with name/hostname "machine-abc" to use 4 threads of Mon-Fri, from 06:00 to 17:00


client.py needs:
* executable llr64 in the same directory
* at least empty work.npg

Setup client/server
-------------------
```
pip install zerorpc
```

Server
------
```
python3 server.py
```

Client
------
```
python3 client.py <default_threads> <server:port> [client_name]
python3 client.py 4 fin.liiv.me:8830
```
