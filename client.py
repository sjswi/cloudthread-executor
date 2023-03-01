# -*- coding: utf-8 -*-
import json
import pickle

import yaml
import base64
import cloudpickle
import requests
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

import yaml

import serialize
import util
from serialize.serialize import SerializeIndependent



class Futures:
    def __init__(self, ip):
        self.task_id = None
        self._result = None
        self.state = None
        self.Ip = "http://"+ip
        pass

    def get(self):
        pass

    #阻塞直到获取结果
    def wait(self):
        while self._result==None:
            self.askServer()
            time.sleep(5)

    def askServer(self):
        resultIp = self.Ip+"/result"
        # print(self.task_id)
        resp = requests.post(resultIp, json.dumps({
            "TaskId": self.task_id,
        }))
        if resp.status_code==502:
            raise Exception("server not start")
        else:
            try:
                con = json.loads(resp.content)
                print(con)
                self.state = con["State"]
                if con["State"] == "error":
                    raise Exception(con["Error"])
                elif con["State"] == "finish":
                    self._result = pickle.loads(str.encode(con["Result"]))
            except json.decoder.JSONDecodeError:
                raise Exception("json serialize error, maybe server not work")

        # else:
        #     self.state = con["State"]
    def result(self):
        self.wait()
        return self._result
    def read(self):
        pass


# class cloudthread:
#
#     def __init__(self, config=None):
#         self.config = config
#         self.num_executors = 0
#         if config is None:
#             self.load_default_config()
#         server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         server.setblocking(False)
#         server.connect(('43.138.60.133', 9090))
#         self.conn = server
#         self.payload = None
#
#     def submit(self, func, args, callback=None) -> Futures:
#         message = {}
#         message['func'] = pickle.dumps(func).decode()
#         message['data'] = pickle.dumps(args).decode()
#         message['command'] = 7
#         self.payload = json.dumps(message)
#         self.send()
#         fu = Futures(conn=self.conn)
#         return fu
#
#     def map(self, func, args, callback=None):
#
#         pass
#
#     def load_default_config(self):
#         with open('cloudthread.yaml', 'r') as f:
#             config = yaml.load(f.read(), yaml.Loader)
#             self.config = config
#
#     def send(self):
#         self.conn.send(self.payload)
def load_config():
    with open("", mode="r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        return config

# 必须得有requirements.txt，需要这个下载对应的依赖层
class cloudthread:

    # submit消息
    # func name
    # func file（pickle-》base64 encode）
    # args（pickle-》base64 encode）
    # dependency 依赖包
    # module 用户自定义的class, func之类的依赖 //字典
    # submit 返回的消息
    # state
    # func name
    def __init__(self, target=None, name=None,
                 args=None, kwargs=None, threadNumber=1):

        #TODO
        # 使用StringsIO去除base64模块
        config = load_config()
        # 最初方案直接使用cloudpickle
        # self.func = base64.b64encode(cloudpickle.dumps(target)).decode()
        # self.args = cloudpickle.dumps(args,0).decode()
        # 使用serialize
        self.module = {}
        self.ignore = []
        #分析requrements.txt
        with open('./requirements.txt', mode="r") as f:
            for line in f.read():
                strs = line.split(" ")
                self.ignore.append([strs[0], True])
                self.module[strs[0]] = strs[1]
        self.serializer = SerializeIndependent(self.ignore)
        strs, modData = self.serializer([target]+[args], [], [])
        self.func = strs[0]
        self.args = strs[1]
        self.Ip = config["cloudthread"]["host"]+":"+str(config["cloudthread"]["port"])
        #TODO 需要验证参数是否符合，函数是否可调用
        util.verify()
        self.funcName = target.__name__
        self.fu = Futures(self.Ip)
        self.task_id = None
        self.threadNum = threadNumber
        # 分析requirements.txt
        self.dependency = json.dumps(self.module)


    def submit(self, target=None, args=None):
        return self.start()
        # return Futures()

    def start(self):
        submitIp = self.Ip+"/submit"
        resp = requests.post(submitIp, json.dumps({
            "FuncName": self.funcName,
            "Args": self.args,
            "FuncFile": self.func,
            "ThreadNumber": self.threadNum,
            "FunctionModule": self.module,
            "FunctionDependency": self.dependency
        }))

        if resp.status_code==502:
            raise Exception("server not start")
        else:
            cont = json.loads(resp.content)
            print(cont)
            # TODO
            # 需要错误处理，暂时留着
            if cont["State"]!="create":
                raise Exception("启动任务异常")
            else:
                self.task_id = cont["TaskId"]
            self.fu.task_id = self.task_id
            return self.fu

    def join(self):
        pass

    #TODO
    # 1、构建DAG程序
    # 2、mapreduce程序


def hello(args):
    print(args)
    return args


if __name__ == "__main__":
    thread = cloudthread(target=hello, args="asdkjsauk")

    fu = thread.submit()
    # print(fu.task_id)
    fu.wait()
    print(fu.result())
