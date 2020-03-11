import zerorpc
import subprocess
import os
import sys
import time
import socket

print("version: 2020-03-10 13:51")

if "linux" in sys.platform:
    EXE_PATH = "./llr64"
else:
    EXE_PATH = "./cllr64.exe"
THREADS = int(sys.argv[1])
HOST = sys.argv[2]

client_name = socket.gethostname()
connect_to = "tcp://{}".format(HOST)

c = zerorpc.Client()
c.connect(connect_to)

while True:
    time.sleep(1)
    work_threads = THREADS
    try:
        f = open("llr.ini", 'r')
        all_done = "WorkDone=1" in f.read()
    except:
        all_done = True

    if all_done:
        try:
            os.remove("lresults.txt")
            os.remove("llr.ini")
            os.remove('work.npg')
        except:
            pass

        try:
            work = c.get_work(client_name)
        except:
            print('get_work failed')
            time.sleep(10)
            continue

        print(work)
        if len(work) == 2:
            work, work_threads = work[0], work[1]

        if work:
            f = open('work.npg', 'w')
            f.write(work[0] + "\n")
            f.write(" ".join([str(work[1]), str(work[2])]))
            f.close()
        else:
            time.sleep(5)
            continue

    cmd = [EXE_PATH, '-d', '-t{}'.format(work_threads), 'work.npg']
    proc = subprocess.Popen(cmd)
    proc.wait()

    if os.path.isfile('lresults.txt'):
        work_file = open("work.npg", "r")
        work_lines = work_file.readlines()
        header = work_lines[0].strip()
        k, n = work_lines[1].split(" ")
        k, n = int(k), int(n)
        work = (header, k, n)

        result = open('lresults.txt', 'r')
        res = result.read()
        result.close()

        while True:
            try:
                c.report_work(client_name, work, res)
                break
            except:
                print("failed to report work: ", work, res)
                time.sleep(10)
                continue
