
import zerorpc
import os

WORK_FILES_DIR = "work"
RESULT_FILES_DIR = "results"
IN_PROGRESS_DIR = "in_progress"

def work_to_name(work):
    sieve_p = work[0].index(":")
    sieve_p_replacement = "0"
    header_line = "0{}".format(work[0][sieve_p:-1])
    line = "{}:{}:{}".format(header_line, work[1], work[2])
    line = line.replace(":", "_")
    return line

def get_next_work():
    work_files = os.listdir(WORK_FILES_DIR)
    done_work = set(os.listdir(RESULT_FILES_DIR))
    in_progress = set(os.listdir(IN_PROGRESS_DIR))

    all_work = []
    for work_file in work_files:
        f = open(os.path.join(WORK_FILES_DIR, work_file), 'r')
        work_file_lines = f.readlines()
        f.close()
        work_file_header = work_file_lines[0].strip()
        work_file_work = work_file_lines[1:]
        for work_line in work_file_work:
            k, n = work_line.split(" ")
            work = (work_file_header, int(k), int(n))
            all_work.append(work)
    available_work = list(filter(lambda x: work_to_name(x) not in done_work and work_to_name(x) not in in_progress, all_work))
    print("all: {} done: {} progress: {} available: {}".format(len(all_work), len(done_work), len(in_progress), len(available_work)))

    if available_work:
        first_available = available_work[0]
        return first_available
    else:
        None


class RPCServer(object):
    def get_work(self, client_name):
        print("-> get work")
        work = get_next_work()
        if not work:
            print("no work to give")
            return None

        work_name = work_to_name(work)

        filepath = os.path.join(IN_PROGRESS_DIR, work_name)
        f = open(os.path.join(IN_PROGRESS_DIR, work_name), 'x')
        f.write(client_name)
        f.close()

        print("giving work {} to {}".format(work, client_name))
        return work

    def report_work(self, client_name, work, result):
        print("-> report work")
        work_name = work_to_name(work)

        print("got result from {} for {} -> {}".format(client_name, work, result))
        result_exists = os.path.isfile(os.path.join(RESULT_FILES_DIR, work_name))
        if result_exists:
            print("{} reported already done work {}".format(client_name, work_name))
            return True

        os.remove(os.path.join(IN_PROGRESS_DIR, work_name))
        f = open(os.path.join(RESULT_FILES_DIR, work_name), 'x')
        f.write(result)
        f.close()

        return True

s = zerorpc.Server(RPCServer())
s.bind("tcp://0.0.0.0:1911")
s.run()
