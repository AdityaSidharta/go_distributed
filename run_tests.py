from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import signal
import subprocess
import sys
import threading
from time import time
import argparse

test_maps = {
    "2a": ("Test1", ),
    "2b": ("TestBasicFail", "TestAtMostOnce", 
            "TestFailPut", "TestConcurrentSame", 
            "TestConcurrentSameUnreliable", "TestRepeatedCrash", 
            "TestRepeatedCrashUnreliable", "TestPartition1", "TestPartition2"),
    "3a": ("TestBasic", "TestDeaf", "TestForget", "TestManyForget", 
            "TestForgetMem", "TestRPCCount", "TestMany", "TestOld", 
            "TestManyUnreliable", "TestPartition", "TestLots"),
    "3b": ("TestBasic", "TestDone", "TestPartition", 
            "TestUnreliable", "TestHole", "TestManyPartition"),
    "4a": ("TestBasic", "TestUnreliable", "TestFreshQuery"),
    "4b": ("TestBasic", "TestMove", "TestLimp", 
            "TestConcurrent", "TestConcurrentUnreliable")
}

LOCK = threading.Lock()
def safe_print(*args):
    with LOCK:
        print(*args)

def run_test(test_name, iter):
    logfile = f"./log-{test_name}-{iter}.txt"
    # remove existing log
    try:
        os.remove(logfile)
    except:
        pass
    # go run test    
    proc = subprocess.Popen(f"""go test -run "^{test_name}$" -timeout 2m > {logfile}""", shell=True)
    safe_print(f"Running {test_name} iteration {iter} in subprocess {proc.pid}...")
    proc.wait()
    exit_code = proc.returncode
    # 0 exit code means test passed
    # Ref: https://github.com/golang/go/issues/25989#issuecomment-399275051
    success = exit_code == 0
    # But... just to check for the PASS string to make sure again
    with open(logfile, 'r') as lf:
        success = success and "PASS" in lf.read()
    # remove log file if success
    if success:
        try:
            os.remove(logfile)
        except:
            pass
    else:
        safe_print(f"{test_name} iteration {iter} FAILED!")
    
    if success:
        return ""
    return logfile

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("subdir", help="Subdirectory under ./src, e.g. pbservice, viewservice, etc")
    parser.add_argument("hw", help="Homework id, e.g. 2a, 2b, 3a, etc")
    parser.add_argument("--selected_tests", "-t", default=None, help="Selected tests in CSV format, e.g. TestBasicFail,TestAtMostOnce")
    parser.add_argument("--clear", "-k", help="Whether to remove existing test dirs.", action="store_true")
    parser.add_argument("--concurrency", "-c", default=4, type=int, help="Number of workers to run tests concurrently")
    parser.add_argument("--runs", "-r", default=50, type=int, help="Number of runs for EACH test")
    args = parser.parse_args()

    subdir, hw, selected_tests = args.subdir, args.hw, args.selected_tests
    concurrency, runs = args.concurrency, args.runs
    if selected_tests is not None:
        selected_tests = set(args.selected_tests.split(','))
    # copy entire dir to avoid impacting current file
    ts = time()
    # remove existing test run dirs
    if args.clear:
        os.system("rm -rf ./_test_run_*")
    # create a new run dir
    runroot = os.path.join(os.getcwd(), f"_test_run_{ts}")
    shutil.copytree("./src", runroot)
    safe_print(f"Switching to subdir {runroot}...")
    # move to subdir
    os.chdir(os.path.join(runroot, subdir))

    # run test in parallel
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        try:
            futs = []
            failed = []
            for t in test_maps[hw]:
                if not selected_tests or t in selected_tests:
                    for i in range(runs):
                        futs.append(executor.submit(run_test, t, i))
            for f in futs:
                t_failed = f.result()
                if t_failed != "":
                    failed.append(t_failed)
            if len(failed) > 0:
                safe_print(f"{len(failed)}/{len(futs)} has failed; See logs below -")
                for t_failed in failed:
                    safe_print(t_failed)
            else:
                safe_print("All test runs have passed!")
        except BaseException as e:
            safe_print(f"Caught exception {e}. Exiting program.")
            os._exit(1)
