import json
import pickle
import select
from socket import *
import socket
import threading
ServerNetworkType = "tcp"
ServerAddress     = "43.138.60.133:9090"
MessageDelimiter  = '\r\n\r\n'
JobQueue = []
SendQueue = []
want_write=True
lock = threading.Lock()


def handleConn(conn):
    global JobQueue, SendQueue

    while True:
        rfds, wfds, afds = select.select([conn], [], [], 0.5)
        for fd in rfds:
            temp_data = Read(fd)
            with lock:
                JobQueue.append(temp_data)
        # if temp_dat
        if want_write:
            send_temp = None
            with lock:
                if len(SendQueue)!=0:
                    send_temp = SendQueue[0]
                    SendQueue.pop(0)
            Write(conn, send_temp)


def writeMessage(conn, message):
    conn.send(message)


def Write(conn: socket.socket, data: str):
    # data = json.dumps({"type":1}) + MessageDelimiter
    # print("发送数据")
    a = pickle.dumps(test_hello, 0).decode()
    # print(type(a))
    temp = {
        "Type": 1,
        "Func": a,
        "Data": "a"
    }
    t = pickle.loads(str.encode(a))
    # print(t(a))
    # print(type(json.dumps(temp)))
    temp = json.dumps(temp)+MessageDelimiter
    temp = temp.encode()
    print(len(temp))
    conn.send(temp)

def analysisCommand(command):
    pass


def processing(func, temp_data):
   func(temp_data)


def Read(conn: socket.socket) -> dict:
    print("接受数据")
    buf = ""
    while True:
        nbuf = conn.recv(1024).decode()
        # json.loads(nbuf)
        # print(len(nbuf))
        if nbuf[-4:]=="\r\n\r\n":
            print("一条语句结束")
            command = buf + nbuf[:-4]
            # print()


            # print(command)
            #     # command = "\"\"\""+command+"\"\"\""
            # print(len(command))
            temp = json.loads(command)
            return temp
            # if temp['Func'] != "":
            #     t = pickle.loads(str.encode(temp['Func']))
            #     data = temp['Data']
            #     print(t(data))
            #     break
            #
            #
            # buf = ""
            # Write(conn, "Hello")
        if len(nbuf)==0:
            break
        buf += nbuf

    return buf


def test_hello(_):
    return "Hello"


if __name__ == '__main__':
    # a = pickle.dumps(test_hello, 0).decode()
    # # print(type(a))
    # temp = {
    #     "Type": 1,
    #     "Func": a,
    #     "Data": "a"
    # }
    # t = pickle.loads(str.encode(a))
    # # print(t(a))
    # print(type(json.dumps(temp)))
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    server.setblocking(False)
    server.connect(('43.138.60.133', 9090))
    threading.Thread(target=handleConn, args=[server,])
    with lock:
        SendQueue.append("hello")
    global func
    while True:
        temp_data = None

        with lock:
            if len(JobQueue)>0:
                temp_data = JobQueue[0]
                JobQueue.pop(0)
        processing(func, temp_data)
