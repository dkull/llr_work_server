import datetime
from time import time
import zerorpc
import os

WORK_FILES_DIR = "work"
RESULT_FILES_DIR = "results"
IN_PROGRESS_DIR = "in_progress"


def work_to_name(work):
    sieve_p = work[0].index(":")
    header_line = "0{}".format(work[0][sieve_p:-1])
    line = "{}:{}:{}".format(header_line, work[1], work[2])
    line = line.replace(":", "_")
    return line


def get_next_work():
    work_files = os.listdir(WORK_FILES_DIR)
    done_files = set(os.listdir(RESULT_FILES_DIR))
    in_progress = set(os.listdir(IN_PROGRESS_DIR))

    all_work = []
    for work_file in work_files:
        f = open(os.path.join(WORK_FILES_DIR, work_file), "r")
        work_file_lines = f.readlines()
        f.close()
        work_file_header = work_file_lines[0].strip()
        work_file_work = work_file_lines[1:]
        for work_line in work_file_work:
            k, n = work_line.split(" ")
            work = (work_file_header, int(k), int(n))
            all_work.append(work)

    available_work = list(
        filter(
            lambda x: work_to_name(x) not in done_files
            and work_to_name(x) not in in_progress,
            all_work,
        )
    )

    active_k = available_work[0][1]

    available_this_k = len(
        list(
            filter(
                lambda x: work_to_name(x) not in done_files
                and work_to_name(x) not in in_progress
                and x[1] == active_k,
                all_work,
            )
        )
    )

    print(
        "{} ... {}".format(
            available_work[0], available_work[-1]
        )
    )

    print(
        "all: {} done: {} progress: {} available: {} available[k={}]: {}".format(
            len(all_work),
            len(done_files),
            len(in_progress),
            len(available_work),
            active_k,
            available_this_k,
        )
    )

    if available_work:
        first_available = available_work[0]
        return first_available
    else:
        None


def print_stats(clients):
    now = time()
    jobs_in_hour = 0.0
    missing_jobs_in_hour = 0.0
    active_clients = 0
    for (name, client) in clients.items():
        work_rate = 3600.0 / client.last_work_duration
        if client.last_seen + (3600 * 6) < now:
            missing_jobs_in_hour += work_rate
            print(
                "client {} not seen for a while now (missing out on {}/h)".format(
                    client.name
                )
            )
            continue
        active_clients += 1
        jobs_in_hour += work_rate
    print(
        "{}/{} active clients process {:.3f}/h {:.2f}/24h, missing out on {}/h".format(
            active_clients,
            len(clients),
            jobs_in_hour,
            jobs_in_hour * 24,
            missing_jobs_in_hour,
        )
    )


def get_or_create_client(clients, name):
    if name in clients:
        return clients[name]
    client = Client(name)
    clients[name] = client
    return client


class Client:
    def __init__(self, name):
        self.name = name
        self.last_seen = 0
        self.last_work_duration = 1.0  # to not get division by zero


class RPCServer(object):
    def __init__(self):
        self.clients = {}
        self.anomaly_seen = False

    def get_work(self, client_name):
        if self.anomaly_seen:
            print("\n-> get work [{}] __ANOMALY_SEEN__".format(client_name))
        else:
            print("\n-> get work [{}]".format(client_name))

        work = get_next_work()
        if not work:
            print("no work to give")
            return None

        work_name = work_to_name(work)

        path = os.path.join(IN_PROGRESS_DIR, work_name)
        f = open(path, "x")
        try:
            # python2
            if isinstance(client_name, bytes):
                client_name = client_name.decode('utf-8')
            f.write(client_name)
            f.close()

            print("giving work {} to {}".format(work, client_name))

            print_stats(self.clients)
            return work
        except:
            f.close()
            print("removing file due to exception: ", path)
            os.remove(path)

    def report_work(self, client_name, work, result):
        result = result.strip()

        # Python2
        if isinstance(work[0], bytes):
            work[0] = work[0].decode('utf-8')
        if isinstance(result, bytes):
            result = result.decode('utf-8')

        print("\n-> report work [{}] {}".format(client_name, datetime.datetime.utcnow().isoformat()))
        work_name = work_to_name(work)

        client = get_or_create_client(self.clients, client_name)
        client.last_seen = time()
        try:
            duration = float(result.split(" ")[-2])
            client.last_work_duration = duration
        except:
            print("couldn't parse work duration from {}".format(result))
            pass

        if "is prime!" in result:
            self.anomaly_seen = True
        print("{} tested {} -> {}".format(client_name, work, result))

        result_exists = os.path.isfile(os.path.join(RESULT_FILES_DIR, work_name))
        if result_exists:
            print("{} reported already done work {}".format(client_name, work_name))
            return True

        os.remove(os.path.join(IN_PROGRESS_DIR, work_name))
        f = open(os.path.join(RESULT_FILES_DIR, work_name), "x")
        f.write(result)
        f.close()

        return True


s = zerorpc.Server(RPCServer())
s.bind("tcp://0.0.0.0:8830")
try:
    s.run()
except:
    # don't throw on Ctrl-c
    pass
