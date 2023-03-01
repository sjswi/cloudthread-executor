# -*- coding: utf-8 -*-
import base64
import json
import pickle
import threading
import time

import requests

WaitingState = "waiting"
RunningState = "running"

# state = 1
# Id = "0000001"
# Finish = False
# lock = threading.Lock()
# ping 消息
# func name string
# state int
# ping 返回的消息
# 5s一次
class Executor:
    def __init__(self, state, id, ip):
        self.state = state          # 执行器状态
        self.Id = id    #执行器id
        self.lock = threading.Lock()    #锁
        self.Finish = False
        self.Waited = ""
        self.TaskId = ""
        # self.time = time.time()
        self.server = "http://"+ip
        # 函数名与函数内容的映射，存储该executor现有的函数，每次新增函数将函数名放进ping消息
        self.funcs = {}

    def ping(self):
        # global Finish
        tempIp = self.server+"/ping"
        while True:
            time.sleep(1)
            # print("sadd")
            self.lock.acquire()
            finish = self.Finish
            self.lock.release()
            if finish:
                # lock.release()
                break
            else:
                # print("sdffdsfsdfdsfdsdfs")
                # print(json.dumps({"Id": self.Id, "State": self.state, "TaskId": self.TaskId}))
                resp = requests.post(tempIp, json.dumps({"Id": self.Id, "State": self.state, "TaskId": self.TaskId}))
                if resp.status_code==502:
                    print("server error")
                else:
                    try:
                        cont = json.loads(resp.content)
                        if cont["Message"]=="pong":
                            print("pong")
                            # lock.release()

                        else:
                            self.lock.acquire()
                            self.Finish = True
                            self.lock.release()
                            break
                    except json.decoder.JSONDecodeError as e:
                        print(e)

    def exec(self):
        connectIp = self.server+"/connect"
        assignIp = self.server+"/assignJob"
        finishIp = self.server+"/finish"
        resp = requests.post(connectIp, json.dumps({
            "Id": self.Id,
            "State": self.state,
            # "StartTime": self.time
        }))
        cons = json.loads(resp.content)
        print(cons)
        if cons["State"] == "success":
            self.state = RunningState
            th = threading.Thread(target=self.ping)
            th.start()
            while True:
                print("asdkjbsa")
                self.lock.acquire()
                finish = self.Finish
                self.lock.release()
                if finish:
                    # print("asdkjbsa")
                    # lock.release()
                    break
                else:
                    # time.sleep(20)
                    # assign 消息
                    # Id string
                    # Time string 还可以运行时间
                    # assign 返回消息
                    #   Id string
                    #   TaskId string
                    #   FuncName string
                    #   FuncFile string
                    #   NewFunc  bool 有新的函数
                    #   Args string
                    #   State string 可能不分配任务直接取消
                    # TODO
                    resp = requests.post(assignIp, json.dumps({
                        "Id": self.Id,
                        "State": self.Waited
                        # "Time": self.time  # 暂时不用，后期添加
                    }))
                    con = json.loads(resp.content)
                    print(con)
                    # fn = con["func"]
                    # args = con["args"]
                    # msg = con["msg"]
                    # 没有任务直接退出
                    if con["State"] == "finish":
                        self.lock.acquire()
                        self.Finish = True
                        self.lock.release()
                        break
                    # 睡眠几秒
                    elif con["State"]=="wait":
                        waitTime = con["WaitTime"]
                        time.sleep(waitTime)
                        self.lock.acquire()
                        self.Waited = "waited"
                        self.lock.release()
                    # 分配到任务，解析执行
                    elif con["State"] == "assigned":
                        # TODO
                        # 使用StringsIO去除base64模块
                        fn = pickle.loads(base64.b64decode(str.encode(con["FuncFile"])))
                        args = pickle.loads(str.encode(con["Args"]))
                        error = ""
                        result = ""
                        state = ""
                        self.lock.acquire()
                        self.TaskId = con["TaskId"]
                        self.lock.release()
                        # duration = ""
                        startTimeStamp = 0
                        endTimeStamp = 0
                        try:
                            startTimeStamp = time.time()
                            result = fn(args)
                            endTimeStamp = time.time()
                            state = "success"
                        except Exception as e:
                            # TODO
                            # 错误处理
                            error = str(e)
                            state = "error"
                        # finish 消息
                        # Result string
                        # TaskId string
                        # Error 有错误信息时设置
                        resp1 = requests.post(finishIp, json.dumps({
                            "Result": pickle.dumps(result, 0).decode(),
                            "TaskId": con["TaskId"],
                            "Error": error,
                            "State": state,
                            "Id": con["Id"],
                            "ExecTime": endTimeStamp-startTimeStamp,
                            "StartTimeStamp": startTimeStamp,
                            "EndTimeStamp": endTimeStamp
                        }))
                        con1 = json.loads(resp1.content)

                        if con1["State"] == "finish":
                            self.lock.acquire()
                            self.Finish = True
                            self.lock.release()
                            break
        else:
            self.lock.acquire()
            self.Finish = True
            self.lock.release()


# 连接的信息
#   Id string
# 连接返回的消息
#   State
#   Id
#
#
def handler(event, context=None):

    exec = Executor(WaitingState, event["Id"], "127.0.0.1:8888")
    exec.exec()
if __name__=="__main__":
    # exec = Executor()
    handler({"Id": "0000001"})