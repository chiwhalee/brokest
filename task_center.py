#!/usr/bin/env python
#coding=utf8

import unittest
import sys 
import threading
from multiprocessing import Process, Queue,  Pool 
import zmq
import time 
import itertools 

from brokest import send_msg, pack_runnable_msg, resolve_server, run_one_in_queue 

from merapy import print_vars

"""
    this is a program very similar to the system task manager
    it should support 调度jobs
    
    design: 
        need a TaskList 容器 
            把job按照优先级排序
            支持重排序
            支持插入
"""

class Job(object): 
    def __init__(self): 
        pass
        self.parent = None
        self.child = None
        self.priority = 0
    
    def unpack(self): 
        pass 

class TaskCenter(object): 
    def __init__(self, **kwargs): 
        default_args= {
                'host': '127.0.0.1', 
                'port': 90999, 
                'info': 0, 
                'start': False, 
                'server_list': [], 
                }
        if 1: 
            for k in default_args: 
                if kwargs.has_key(k): 
                    default_args[k] = kwargs[k]
            
            for k, v in default_args.iteritems(): 
                setattr(self, k, v)
        
        self.server_cycle = itertools.cycle(self.server_list)
        
        self.task_list = []
        self.completed_tasks = []
        
        self.task_list_lock = threading.Lock()
        if self.start: 
            self.threads= []
            self.receive_job()
            
            t = threading.Thread(target=self.receive_job, args=())
            self.threads.append(t)
            t = threading.Thread(target=self.send_job, args=())
            self.threads.append(t)
            
            for t in self.threads: 
                t.start()
            
            for t in self.threads: 
                t.join()
        
    def receive_job(self): 
        print 'start receive_job'
        self._context = zmq.Context()
        self.socket = self._context.socket(zmq.REP)
        self.socket.bind('tcp://{}:{}'.format(self.host, self.port))
        while True:
            is_response = True
            msg = self.socket.recv_pyobj()
            header = msg['header']
            if header == 'run': 
                p = msg.get('priority', 0)
                with self.task_list_lock: 
                    i = 0
                    for i, t in enumerate(self.task_list): 
                        if p >= t.get('priority', 0) : 
                            break
                    self.task_list.insert(i, msg)
                            
            elif header == 'job_group': 
            #job.priority = ?
                job = msg['job_group']
                print_vars(vars(),  ['msg["priority"]'])
                with self.task_list_lock: 
                    self.task_list.append(job)
            elif header == 'get_job_list' : 
                with self.task_list_lock: 
                    self.socket.send_pyobj(self.task_list)
                    is_response = False
            elif header == 'stop' : 
                print 'stop server now'
                break 
                
            response = 'received'
            if is_response: 
                self.socket.send_pyobj(response)
            time.sleep(0.5)            
    
    def send_job(self): 
        while True: 
            task_sent = False
            try: 
                with self.task_list_lock: 
                    #for t in self.task_list: 
                    #    print t
                    #pass
                    t = self.task_list[0]
                #for s in self.server_list: 
                status= 'run'
                print 'distribute task ... '
                while status != 'done': 
                    s = self.server_cycle.next()
                    if 1: 
                        num_of_threads = t['kwargs'].get('num_of_threads', 1)
                        num_of_memory = t['kwargs'].get('num_of_memory')
                        msg = {
                            'header': 'querry', 
                            'what': 'is_available', 
                            #'num_of_threads': kwargs.get('NUM_OF_THREADS', None),   # requested resources 
                            #'num_of_memory': kwargs.get('num_of_memory', None), 
                            'num_of_threads': num_of_threads, 
                            'num_of_memory': num_of_memory, 
                        }  
                        server_status = send_msg(msg, server=s) # in ['available', 'not_reachable']
                        
                    print 'trying server {}: status={}'.format(s, server_status)
                        
                    if server_status == 'not_reachable' : 
                        continue 
                    elif server_status == 'available': 
                        status = send_msg(t, server=s)
                        print 'task is sent'
                   
                    time.sleep(1)
            except Exception as err: 
                print err
                break 
            time.sleep(0.5)

    def send_job_test(self): 
        while True: 
            task_sent = False
            try: 
                with self.task_list_lock: 
                    #for t in self.task_list: 
                    #    print t
                    #pass
                    t = self.task_list[0]
                #for s in self.server_list: 
                run_one_in_queue(t, self.server_cycle)
               
                
            except Exception as err: 
                print err
                break 
            time.sleep(0.5)

    def get_job_list(self): 
        #with self.task_list_lock: 
        #    pass
        pass
    
def submit_many(job_group, job_group_name='', priority=0): 
    msg = { 'header': 'job_group', 
            'job_group': job_group, 
            'job_group_name': job_group_name, 
            'priority': priority, 
            }
    server = ('127.0.0.1', 90999)
    status = send_msg(msg, server=server)
    #print_vars(vars(),  ['status'])
    print 'status is {}'.format(status)

def submit_one(func, args=None, kwargs=None,  job_group_name='', priority=0): 
    args = args if args is not None else () 
    kwargs = kwargs if kwargs is not None else {}
    msg = pack_runnable_msg(func, args, kwargs)
    msg.update( { 
            'job_group_name': job_group_name, 
            'priority': priority, 
            })
    server = ('127.0.0.1', 90999)
    status = send_msg(msg, server=server)
    #print_vars(vars(),  ['status'])
    print 'status is {}'.format(status)


class TestIt(unittest.TestCase): 
    INITED = False 
    #def setUp(self): 
    @classmethod 
    def setUpClass(cls): 
        from brokest import Worker 
        cls.tt = []
        t = Process(target=Worker, args=(), 
                kwargs=dict(host='127.0.0.1', port=90911, start=1, 
                    num_of_cpu=4))
        cls.tt.append(t)
        
        t = Process(target=TaskCenter, args=(), 
                kwargs={'start': 1, 
                    'server_list':[('127.0.0.1', 90911)], 
                    }) 
        cls.tt.append(t)
        
        for t in cls.tt: 
            t.start()
   
    #def tearDown(self): 
    @classmethod
    def tearDownClass(cls): 
        send_msg(msg={'header': 'stop'}, server=('127.0.0.1', 90999))
        send_msg(msg={'header': 'stop'}, server=('127.0.0.1', 90911))
        for t in cls.tt: 
            t.join()
    
    def test_temp(self): 
        job_group = ['a' for i in range(10) ]
        def f(): 
            print 'hello'
        #submit_many(job_group)
        for i in range(10): 
            submit_one(f)
        res=send_msg(msg={'header': 'get_job_list'}, server=('127.0.0.1', 90999))
        #print 'rrr', res 
        


if __name__ == '__main__':
    
    
    if len(sys.argv)>1: 
        parser = argparse.ArgumentParser(description='dont know')
        parser.add_argument('-s', '--start', action='store_true', default=False) 
        parser.add_argument('-m', '--mode', default='ansync') 
        parser.add_argument('-p', '--port', type=int, default=None)
        args = parser.parse_args()
        args = vars(args)
        port = args['port']
        mode = args['mode']
        from config import config 
        cfg = config.copy()
        if port is not None: 
            cfg.update(port=port)
        cfg.update(mode=mode, start=1)
        if args['start']: 
            #w = Worker(port=port, mode=mode, start=1)
            w = Worker(**cfg)
            #w.start()
            #w.start_new()
            #w.start_async()
                
    else: 
        #sr = ThreadedServer(TestRpyc, port=9999, auto_register=False)
        
        if 0:
            TestIt.test_temp=unittest.skip("skip test_temp")(TestIt.test_temp) 
            unittest.main()
            
        else: 
            suite = unittest.TestSuite()
            add_list = [
                'test_temp', 
                
            ]
            for a in add_list: 
                suite.addTest(TestIt(a))

            unittest.TextTestRunner(verbosity=0).run(suite)
    
    

