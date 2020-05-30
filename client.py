import datetime
import zerorpc
import subprocess
import os
import sys
import time
import socket

client_name = socket.gethostname()

print("version: 2020-04-14 18:21")
print(sys.argv)

f = open("/proc/cpuinfo", "r")
lines = f.read().split("\n")
f.close()
cpus = list(filter(lambda x: "CPU" in x, lines))
print("{} x {}".format(len(cpus), cpus[0].split(":")[1]))

if sys.argv[1] == "-f":
    THREADS = 2
    HOST = "<host>:<port>"
else:
    THREADS = int(sys.argv[1])
    HOST = sys.argv[2]
    if len(sys.argv) >= 4:
        client_name = sys.argv[3]

print("hostname {}".format(client_name))

is_cuda = False
if "linux" in sys.platform:
    if os.path.isfile("./llrCUDA"):
        is_cuda = True
        EXE_PATH = "./llrCUDA"
    else:
        EXE_PATH = "./llr64"
else:
    EXE_PATH = "./cllr64.exe"


def run(server, hostname, threads):
    print("running...")

    connect_to = "tcp://{}".format(server)
    c = zerorpc.Client()
    c.connect(connect_to)

    use_threads = threads
    while True:
        time.sleep(1)
        now = datetime.datetime.now().__str__()[:22]
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
                work = c.get_work(hostname)
            except:
                print(f'{now}: get_work failed')
                time.sleep(10)
                continue

            if isinstance(work, dict):
                got_state_file = 'state' in work and work['state'] is not None
                if got_state_file:
                    state_file_name, state_file_data = work['state']
                    f = open(state_file_name, 'wb')
                    f.write(state_file_data)
                    f.close()
                work = work['work']
                print(f"{now}: got resumable work from server {work} state? {got_state_file}")
            else:
                print(f"{now}: {work}")
                if len(work) == 2:
                    work, use_threads = work[0], work[1]
                else:
                    use_threads = threads

            if work:
                f = open('work.npg', 'w')
                f.write(work[0] + "\n")
                f.write(" ".join([str(work[1]), str(work[2])]))
                f.close()
            else:
                time.sleep(5)
                continue
        else:
            print(f"{now}: resuming from local offline files...")

        if is_cuda:
            cmd = [EXE_PATH, '-d', 'work.npg']
        else:
            cmd = [EXE_PATH, '-d', '-t{}'.format(use_threads), '-oDiskWriteTime=1', 'work.npg']

        proc = subprocess.Popen(cmd)
        i = 0
        while True:
            time.sleep(10)
            now = datetime.datetime.now().__str__()[:22]
            if i % 6 == 0:
                files = os.listdir("./")
                for f in files:
                    if f.startswith("z"):
                        try:
                            _f = open(f, 'rb')
                            data = _f.read()
                            _f.close()
                        except:
                            break
                        try:
                            c.report_progress(hostname, f, data)
                        except Exception as e:
                            print(f"{now}: failed to report progress: {e}")
            retcode = proc.poll()
            i = (i + 1) % 6
            if retcode is not None:
                break
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
                now = datetime.datetime.now().__str__()[:22]
                try:
                    if not is_cuda:
                        c.report_work(hostname, work, res)
                        print(f"{now}: success report work: ", work, res)
                    break
                except:
                    print(f"{now}: failed to report work: ", work, res)
                    time.sleep(2)
                    continue

def mymain():
    run(HOST, client_name, THREADS)

if __name__ == '__main__':
    mymain()
