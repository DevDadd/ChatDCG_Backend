import os, sys, threading, time
import grpc_server
import grpc_stream_test as T

threading.Thread(target=grpc_server.serve, daemon=True).start()
time.sleep(2.0)
os.environ['GRPC_TEST_TARGET'] = 'localhost:50051'
T.TARGET = 'localhost:50051'
T.TIMEOUT = 15.0

rc = 0
try:
    T.main()
except Exception as e:
    import traceback
    traceback.print_exc()
    rc = 1

sys.stdout.flush()
sys.stderr.flush()
time.sleep(0.3)
os._exit(rc)
