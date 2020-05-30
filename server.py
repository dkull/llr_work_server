import gc
import datetime
from time import time
import zerorpc
import os
import sys

WORK_FILES_DIR = "work"
RESULT_FILES_DIR = "results"
IN_PROGRESS_DIR = "in_progress"

def scheduled_threads(client_name):
    now = datetime.datetime.today()
    day_of_week = now.weekday()
    current_hour = now.hour
    with open('schedule.lst', 'r') as f:
        lines = f.readlines() 
        for line in lines:
            tokens = line.split(":")
            name, threads, days, hours = tokens
            s_begin, s_hours = map(int, hours.split("+"))
            hour_range = [int(h) % 24 for h in range(s_begin, s_begin + s_hours)]
            if name != client_name:
                continue
            if str(day_of_week) not in days:
                continue
            if current_hour not in hour_range:
                continue
            return int(threads)

    return None

def read_clients_from_file():
    f = open('servers.txt', 'r')
    data = f.readlines()
    f.close()
    clean_names = []
    for line in data:
        name = line.strip()
        if '#' in line:
            name = line[:line.index('#')].strip()
        if len(name) <= 1:
            continue
        clean_names.append(name)
    return clean_names


def work_to_name(work):
    sieve_p = work[0].index(":")
    header_line = "0{}".format(work[0][sieve_p:-1])
    line = "{}:{}:{}".format(header_line, work[1], work[2])
    line = line.replace(":", "_")
    return line


def get_next_work(client_name):
    work_files = os.listdir(WORK_FILES_DIR)
    done_files = set(os.listdir(RESULT_FILES_DIR))
    in_progress = set(os.listdir(IN_PROGRESS_DIR))

    for in_prog in in_progress:
        f = open(os.path.join(IN_PROGRESS_DIR, in_prog), "r")
        data = f.readlines()[0]
        f.close()
        if client_name == data.strip():
            header = "1111111111:M:1:2:258"
            # 0_M_1_2_25_193_3083303
            _,_,_,_,_,k,n = in_prog.split("_")
            work = (header, int(k), int(n)), True
            return work

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

    # sort by n
    available_work.sort(key=lambda x: x[2])

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
        return first_available, False
    else:
        None


def print_stats(clients):
    now = time()
    jobs_in_hour = 0.0
    missing_jobs_in_hour = 0.0
    active_clients = 0
    reporting_clients = 0

    static_client_names = read_clients_from_file()
    for client_name in static_client_names:
        client = clients.get(client_name, None)

        # set defaults
        work_rate = -1.0
        last_reported = -1.0
        last_completion = -1.0

        # calc stuff
        if client:
            if client.last_work_duration != -1:
                work_rate = 3600.0 / client.last_work_duration
            if client.last_completed != -1:
                last_completion = now - client.last_completed
            if client.last_reported != -1:
                last_reported = now - client.last_reported

        completion_time_thresh = 3600
        if client and client.last_work_duration != -1:
            completion_time_thresh = client.last_work_duration * 1.2
        last_completion_recently = last_completion >= 0 and last_completion < completion_time_thresh
        last_report_recently = last_reported >= 0 and last_reported < 60 * 5
        if last_report_recently or (last_completion_recently and (last_report_recently or last_reported == -1)):
            active_clients += 1
            if work_rate != -1.0:
                reporting_clients += 1
                jobs_in_hour += work_rate

        # format stuff for printing
        report_status = "{}".format("R" if not last_report_recently else " ")
        if report_status == "R" and last_completion_recently:
            report_status = "."
        completion_status = "{}".format("C" if not last_completion_recently else " ")
        if completion_status == "C" and last_report_recently:
            completion_status = "."

        last_report_repr = "{:>4.0f}".format(last_reported) if last_reported != -1 else "    "
        last_completed_repr = "{:>4.0f} [{:.2f}]".format(last_completion, last_completion/client.last_work_duration) if client and last_completion != -1 else "    "
        rate_repr = "{:>5.1f}".format(work_rate) if work_rate != -1 else "      "

        print("{}{} client {:>16} last completed {} reported {} rate {}".format(
            report_status, completion_status, client_name, last_completed_repr, last_report_repr, rate_repr))

    for client_name in clients.keys():
        if client_name not in static_client_names:
            print("have unseen client: {}".format(client_name))
    print("active static clients: {}/{}/{} rates: {:.2f}/h {:.2f}/24h".format(reporting_clients, active_clients, len(static_client_names), jobs_in_hour, jobs_in_hour*24))


def get_or_create_client(clients, name):
    if name in clients:
        return clients[name]
    client = Client(name)
    clients[name] = client
    return client


class Client:
    def __init__(self, name):
        self.name = name
        self.last_got_work = -1
        self.last_completed = -1
        self.last_reported = -1
        self.last_work_duration = -1
        self.state_file_name = None
        self.state_file_data = None
        self.state_file_time = None

class RPCServer(object):
    def __init__(self):
        self.clients = {}
        self.anomaly_seen = False

    def get_work(self, client_name):
        # python2
        if isinstance(client_name, bytes):
            client_name = client_name.decode('utf-8')

        if self.anomaly_seen:
            print("\n-> get work [{}] ########## ANOMALY_SEEN ###########".format(client_name))
        else:
            print("\n-> get work [{}]".format(client_name))

        client = get_or_create_client(self.clients, client_name)
        client.last_got_work = time()

        work, resume = get_next_work(client_name)
        if not work:
            print("no work to give")
            sys.stdout.flush()
            return None
        if resume:
            client = self.clients.get(client_name, None)
            if client.state_file_name is not None:
                print("giving resume with state for {} {}".format(client_name, work))
                (when, resume_name, resume_file) = client.state_file_time, client.state_file_name, client.state_file_data
                sys.stdout.flush()
                return {'work': work, 'state': (resume_name, resume_file)}
            else:
                print("giving resume without state for {} {}".format(client_name, work))
                sys.stdout.flush()
                return {'work': work}

        work_name = work_to_name(work)
        path = os.path.join(IN_PROGRESS_DIR, work_name)
        f = open(path, "x")

        try:
            f.write(client_name)
            f.close()

            try:
                print_stats(self.clients)
            except Exception as e:
                print("stats exception: {}".format(e))

            threads = scheduled_threads(client_name)

            giving_work = "giving work {} to {}".format(work, client_name)
            if threads:
                giving_work += " [threads {}]".format(threads)
            print(giving_work)

            if threads:
                sys.stdout.flush()
                return [work, threads]
            else:
                sys.stdout.flush()
                return work
        except Exception as e:
            f.close()
            print("removing file due to exception: ", path, e)
            os.remove(path)

    def report_progress(self, client_name, state_file_name, state_file):
        # python2
        if isinstance(client_name, bytes):
            client_name = client_name.decode('utf-8')
        #print("\n-> report progress [{}] {}".format(client_name, datetime.datetime.utcnow().isoformat()))
        now = time()
        client = get_or_create_client(self.clients, client_name)
        client.state_file_name = state_file_name
        client.state_file_data = state_file
        client.last_reported = now
        gc.collect()
        sys.stdout.flush()

    def report_work(self, client_name, work, result):
        # python2
        if isinstance(client_name, bytes):
            client_name = client_name.decode('utf-8')

        result = result.strip()

        # Python2
        if isinstance(work[0], bytes):
            work[0] = work[0].decode('utf-8')
        if isinstance(result, bytes):
            result = result.decode('utf-8')

        print("\n-> report work [{}] {}".format(client_name, datetime.datetime.utcnow().isoformat()))
        work_name = work_to_name(work)

        client = get_or_create_client(self.clients, client_name)
        client.last_completed = time()
        client.state_file_name = None
        client.state_file_data = None
        client.state_file_time = None
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
            sys.stdout.flush()
            return True

        os.remove(os.path.join(IN_PROGRESS_DIR, work_name))
        f = open(os.path.join(RESULT_FILES_DIR, work_name), "x")
        f.write(result)
        f.close()

        gc.collect()

        sys.stdout.flush()
        return True


while True:
    try:
        s = zerorpc.Server(RPCServer())
        s.bind("tcp://0.0.0.0:8830")
        s.run()
    except Exception as e:
        # don't throw on Ctrl-c
        print(e)
print("EXITING???")
