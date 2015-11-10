#!/usr/bin/env python
#coding=utf8

"""Broker-less distributed task queue."""
import cPickle as pickle 
import numpy as np 
from multiprocessing import Process, Queue,  Pool 
import unittest 
import os 
import sys 
import socket 
import argparse 
import traceback 
import time 
import itertools 
from collections import OrderedDict 
import threading
import thread 


#from stopit import SignalTimeout  as Timeout
#from stopit import ThreadingTimeout  as Timeout
#import stopit 


import zmq
import zmq.ssh 
import cloud

from merapy.utilities import print_vars

#issue: this is alkward, use dict instead 
#from config import HOST, PORT , BUFF_SIZE, NUM_OF_CPU
from config import config 


if 1: 
    def run_one(func, arg, kwarg):   #def run(func, *arg, **kwarg): 
        print 'pid = %d'%(os.getpid())
        func(*arg, **kwarg)

    def run_one_async(func, queue,  arg, kwarg): 
        #print 'pid = %d'%(os.getpid())
        try: 
            func(*arg, **kwarg)
        except Exception as err: 
            raise err 
        finally: 
            get = queue.get()
        #print 'get', get 

    def run_one_with_return(func, queue,  arg, kwarg): 
        res=func(*arg, **kwarg)
        queue.put(res)

class Worker(object):
    """
        A remote task executor.
    """
    #def __init__(self, host='127.0.0.1', port=90900, start=False, info=0):
    def __init__(self, host=None, port=None, mode='ansync', start=False, **kwargs):
        """
            Initialize worker.
        """
        default_args = {
            #core
                #'host': '127.0.0.1', 
                #'port': '90900', 
                'buff_size': 1, 
                'num_of_cpu': 2, 
            #handle idle
                'long_idle_shut_down': False, 
                'idle_time_limit': np.inf,   # if this is set smaller than inf, server shut donw if idle for time exceeds this 
                #'exit_on_long_idle': False, 
                'check_idle_period': 60*5, 
            #misc 
                'info': 0, 
            }
        if 1: 
            for k in default_args: 
                if kwargs.has_key(k): 
                    default_args[k] = kwargs[k]
            
            for k, v in default_args.iteritems(): 
                setattr(self, k, v)
        
        
        self.host = host if host is not None else '127.0.0.1'
        self.port = port if port is not None else 90900 
        #self.buff_size = BUFF_SIZE 
        if self.host is None: 
            self.host = '127.0.0.1'

        
        print 'servering at {} mode={}'.format((host, self.port), mode)
        self._context = zmq.Context()
        self.socket = self._context.socket(zmq.REP)
        self.is_available = True   # 是否有空闲cpu 可用
        

        self.buff = Queue(self.buff_size)  
        self.SHUT_DOWN = False
        
        if self.long_idle_shut_down: 
            self.start_supervise_thread()
        
        if start: 
            #self.start()
            #self.start_new()
            if mode == 'simple' : 
                self.start()
            elif mode == 'ansync': 
                self.start_async()
            elif mode == 'with_return': 
                self.start_with_return()
            else: 
                raise 
        
        if hasattr(self, '_thread_supervise'): 
            self._thread_supervise.join()
        
    def start(self):
        """
            start listening for tasks
        """
        self.socket.bind('tcp://{}:{}'.format(self.host, self.port))
        
        while True:
            runnable_string, args, kwargs = self.socket.recv_pyobj()
            runnable = pickle.loads(runnable_string)
                
            response = self._do_work(runnable, args, kwargs)
            self.socket.send_pyobj(response)
            
            if runnable.__name__ == 'stop' : 
                break 
    
    def start_new(self):
        """
            Start listening for tasks
        """
        self.socket.bind('tcp://{}:{}'.format(self.host, self.port))
        
        while True:
            temp = []
            for i in range(3): 
                #print 'iiii', i 
                runnable_string, args, kwargs = self.socket.recv_pyobj()
                self.socket.send_pyobj('') 
                runnable = pickle.loads(runnable_string)
                temp.append((runnable, args, kwargs))
            #pool = Pool(3)
            pp = []
            
            for t in temp: 
                func, arg, kwarg = t
                #pool.apply_async(func,  (arg, ))
                #pool.close()   
                #pool.join()   # the above two lines are needed to prevent main thread exit
                #p=Process(target=func, args=arg, kwargs=kwargs)#.start()
                p=Process(target=run_one, args=(func, arg, kwarg))#.start()
                pp.append(p)
            for p in pp: 
                p.start()
            print [p.is_alive() for i in pp]
            for p in pp: 
                p.join()
            print [p.is_alive() for i in pp]
            
            #response = self._do_work(runnable, args, kwargs)
            #for i in range(3):  
            #    self.socket.send_pyobj('')
            if runnable.__name__ == 'stop' : 
                break 
    
    def start_async_old(self):
        """Start listening for tasks."""
        self.socket.bind('tcp://{}:{}'.format(self.host, self.port))
        num_proc_max = 3
        pp = []  #a simple queue by hand 
        while True:
            if len(pp)<= num_proc_max: 
                print 'pppp', len(pp)
                runnable_string, args, kwargs = self.socket.recv_pyobj()
                self.socket.send_pyobj('') 
                runnable = pickle.loads(runnable_string)
                p=Process(target=run_one, args=(runnable, args, kwargs))
                p.start()
                pp.append(p)
            else: 
                for i, p in enumerate(pp): 
                    if not p.is_alive(): 
                        pp.pop(i)
                        break 
                time.sleep(1)
                #print 'qqq', len(pp)
            
            #response = self._do_work(runnable, args, kwargs)
            #for i in range(3):  
            #    self.socket.send_pyobj('')
            if runnable.__name__ == 'stop' : 
                break 

    def start_async(self):
        """
            Start listening for tasks.
            用 multiprocessing.Queue 来实现并行化, 这样并行是async的。
            buff is 进程池
        """
        self.socket.bind('tcp://{}:{}'.format(self.host, self.port))
        
        count = 0
        while True:
            recv_dict = self.socket.recv_pyobj()
            assert isinstance(recv_dict, dict)
            
            header = recv_dict['header'] 
            
            #isa = self.is_available_func()
            if header == 'querry':
                querry_what = recv_dict.get('what')
                #if querry_what is None or querry_what == 'is_available':  
                if querry_what == 'is_available':  
                    isa, reason = self.is_available_func(cpu_request=recv_dict.get('num_of_threads'))
                    #if not self.buff.full(): 
                    if isa: 
                        reply = 'available'
                    else: 
                        #reply = 'not available, spleep 0.5sec'
                        reply = 'reason is %s. spleep 0.5sec'%(reason, )
                        self.is_available = False 
                        time.sleep(0.5)  #issue:  this should be not needed
                elif querry_what == 'is_exists': 
                    reply = 'Y'
                else: 
                    raise 
            
            elif header == 'run': 
                runnable_string = recv_dict['runnable_string']
                runnable = pickle.loads(runnable_string)
                args = recv_dict['args']
                kwargs = recv_dict['kwargs']
                count += 1
                if self.info>0: 
                    #print 'put in queue count %d'%(count, )
                    print 'put in queue count %d port=%d'%(count, self.port)
                self.buff.put(count)
                p=Process(target=run_one_async, args=(runnable, self.buff, args, kwargs))
                p.start()
                #p.join()  note:  there should be no join here,  or else it wont be parallel and ansync 
                reply = 'start running'
            
            elif header == 'stop' : 
                reply = 'STOPE SERVER NOW'
                self.socket.send_pyobj(reply) 
                break 
            else: 
                raise 
            self.socket.send_pyobj(reply) 

    def start_with_return(self):
        """
            Start listening for tasks.
            用 multiprocessing.Queue 来实现并行化, 这样并行是async的。
            buff is 进程池
        """
        self.socket.bind('tcp://{}:{}'.format(self.host, self.port))
        buff_size = 3
        buff = Queue(buff_size)  
        count = 0
        while True:
            #todo:  不用buff.full 来判断，而用cpu是否空闲判断
            #querry = self.socket.recv_pyobj()
            recv_dict = self.socket.recv_pyobj() 
            header = recv_dict['header']
            
            if not buff.full():
                #if querry == 'querry' : 
                if header == 'querry':  
                    if not buff.full(): 
                        self.socket.send_pyobj('available') 
                    else: 
                        self.socket.send_pyobj('not available') 
                        self.is_available = False 
                        time.sleep(1)
                    #temp = self.socket.recv_pyobj()
                    #runnable_string, args, kwargs = temp  
                elif header == 'run': 
                    runnable_string = recv_dict['runnable_string']
                    runnable = pickle.loads(runnable_string)
                    args = recv_dict['args']
                    kwargs = recv_dict['kwargs']
                    

                    #args= pickle.loads(args)
                    count += 1
                    if self.info>0: 
                        #print 'put in queue count %d'%(count, )
                        print 'put in queue count %d port=%d'%(count, self.port)
                    #buff.put(count)
                    #p=Process(target=run_one_async, args=(runnable, buff, args, kwargs))
                    p=Process(target=run_one_with_return, args=(runnable, buff, args, kwargs))
                    p.start()
                    p.join()   #Block the calling thread until the process whose join() method is called terminates or until the optional timeout occurs.
                    res = buff.get()
                    self.socket.send_pyobj(res) 
                elif header == 'stop':  
                    self.socket.send_pyobj('stop server') 
                    break 

    def _do_work(self, task, args, kwargs):
        """
            Return the result of executing the given task.
        """
        #print('Running {} with args [{}] and kwargs [{}]'.format(
        #    task.__name__, args, kwargs))
        
        try: 
            return task(*args, **kwargs)
        except Exception as err: 
            print err 
    
    def start_supervise_thread(self): 
        #thread.start_new_thread(self.supervise, ( ))
        self._thread_supervise = threading.Thread(target=self.supervise, args=( ))
        self._thread_supervise.start()
        print 'supervise thread started, check_idle_period={}, idle_time_limit={}'.format(
                self.check_idle_period, self.idle_time_limit)
    
    def supervise(self): 
        """
            use a thread to supervise the status of worker, 
                
                it may be used i.e. to termenate the worker if 
                the worker is idle for a long time 
        """
        idle_count = 0
        while True: 
            if self.buff.empty(): 
                idle_count += 1
                print 'idle_count', idle_count 
                idle_time =  idle_count*self.check_idle_period
                if idle_time > self.idle_time_limit: 
                    #self.SHUT_DOWN = True
                    print 'server idle exceed {} sec, shutdown now'.format(self.idle_time_limit, )
                    self.send_shut_down()
                    break  #exit 
            else: 
                idle_count = 0
            time.sleep(self.check_idle_period)
    
    def is_available_func(self, cpu_request=None, info=0): 
        load = os.getloadavg()[0]
        isa = True 
        cpu_request = cpu_request if cpu_request is not None else 1
        load1 = load + cpu_request 
        reason = 'None'
       
        if load1 > self.num_of_cpu + 0.5:  # here +0.5 may be better( or worse), I am not sure 
            isa = False
            reason = 'cpu not enough. current load is {}, requested num cpu is {}, num_of_cpu is {}'.format(
                    load,  cpu_request, self.num_of_cpu, )
        if self.buff.full(): 
            isa = False
            reason = 'buff is full'
        return isa, reason  
    
    def send_shut_down(self): 
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        url = 'tcp://{}:{}'.format(self.host, self.port)
        socket.connect(url)
        socket.send_pyobj({'header': 'stop'})    

def pack_runnable_msg(func, args=None, kwargs=None): 
    args= args if args is not None else () 
    kwargs = kwargs if kwargs is not None else {}
    
    runnable_string = cloud.serialization.cloudpickle.dumps(func)
    
    msg = {
        'header': 'run', 
        'runnable_string': runnable_string, 
        'args': args, 
        'kwargs': kwargs
        }
    return msg 
    
def queue(runnable, args=None, kwargs=None, querry=False, 
        host=None, port=None, tunnel=False, querry_timeout=5, 
        tunnel_server=None, 
        kill_server=False, ):
    """
        return the result of running the task *runnable* with the given 
        arguments.
        
        params: 
            host: e.g. '210.45.117.30' or 'qtg7501' if use the later should add ip hostname pair in /etc/hosts
            querry:  querry whether server available 
            querry_timeout: 
                我曾经试过用 stopit module 来给recv设置timeout, 但是没有成功，应该是涉及到背后线程没有关闭
                refer to https://github.com/zeromq/pyzmq/issues/132
    """
    #host =  '222.195.73.70'
    #port = 90900
    host = host if host is not None else '127.0.0.1'
    port = port if port is not None else 90900
    args = args if args is not None else ()
    kwargs = kwargs if kwargs is not None else {}
    
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    url = 'tcp://{}:{}'.format(host, port)
    if not tunnel:  #one should either use connect or tunnel_connection, not both.
        socket.connect(url)
    else:
        #zmq.ssh.tunnel_connection(socket, url, "myuser@remote-server-ip")
        #issue:  似乎tunnel对port有限制，不能用90900这样5位数的的端口
        zmq.ssh.tunnel_connection(socket, url, tunnel_server)
        #print 'tunnel succeed: {}'.format(url)
       
    
    if kill_server: 
        socket.send_pyobj({'header': 'stop'}) 
        rep=server_status = socket.recv_pyobj()
        print 'REP: %s'%(rep, )
        return 
        
    results = None 
    status = 'refuse'
    
    if querry:
        socket.setsockopt(zmq.LINGER, 0)   #this is needed or else timeout wont work 
        #socket.send_pyobj('querry')    
        num_of_threads = None
        num_of_memory = None 
        if len(args)>0:  #in main.Main.run_many_dist, NUM_OF_THREADS is passed in args[0]
            if isinstance(args[0], dict): 
                num_of_threads = args[0].get('NUM_OF_THREADS')
                num_of_memory = args[0].get('num_of_memory', None) 
        socket.send_pyobj({
            'header': 'querry', 
            'what': 'is_available', 
            #'num_of_threads': kwargs.get('NUM_OF_THREADS', None),   # requested resources 
            #'num_of_memory': kwargs.get('num_of_memory', None), 
            'num_of_threads': num_of_threads, 
            'num_of_memory': num_of_memory, 
            })    
        # use poll for timeouts:
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        if poller.poll(querry_timeout*1000): # 10s timeout in milliseconds
            server_status = socket.recv_pyobj()
            #print_vars(vars(),  ['server_status'])
        else: 
            
            #raise IOError("Timeout processing auth request")        
            #not able to reach server within querry_timeout 
            #some times, need to enlarge querry_timeout to ensure connection success 
            server_status = 'not_reachable'
            status = 'conn timeout'

        if 0:  # below not working  
            try: 
                with stopit.SignalTimeout(querry_timeout, False) as ctx:
                #with stopit.ThreadingTimeout(querry_timeout, False) as ctx:
                    print 'tttttry', port, host  
                    server_status = socket.recv_pyobj()
            except Exception as err: 
                print 'rrrraise', err 
                #socket.close()
                #context.term()
                #raise 
                #server_status = 'not_reachable'
                raise 
            print 'sssssss', ctx.state    
            if ctx.state == ctx.EXECUTED:
                pass # All's fine, everything was executed within 10 seconds
            elif ctx.state == ctx.EXECUTING:
                pass # Hmm, that's not possible outside the block
            elif ctx.state == ctx.TIMED_OUT:
                server_status = 'not recheable' # Eeek the 10 seconds timeout occurred while executing the block
            elif ctx.state == ctx.INTERRUPTED:
                pass 
                # Oh you raised specifically the TimeoutException in the block
            elif ctx.state == ctx.CANCELED:
                pass # Oh you called to_ctx_mgr.cancel() method within the block but it # executed till the end
            else:
                pass 
                # That's not possible            
            #print 'aaaaaafter ', ctx.state == ctx.TIMED_OUT , ctx.state == ctx.EXCUTING 
            print 'aaaaaafter ', ctx.state == ctx.TIMED_OUT , ctx.TIMED_OUT,  
    
    else:   
        server_status = 'available'
        
    if server_status == 'available': 
        #runnable_string = cloud.serialization.cloudpickle.dumps(runnable)
        #socket.send_pyobj({'header': 'run', 
        #    'runnable_string': runnable_string, 
        #    'args': args, 
        #    'kwargs': kwargs
        #    })    
        msg = pack_runnable_msg(runnable, args, kwargs)
        socket.send_pyobj(msg)
       
        results = socket.recv_pyobj()
        status = 'done'
    else: 
        if server_status != 'not_reachable':
            status += '  %s'%(server_status, ) 


    
    # these are not necessary, but still good practice:
    socket.close()
    context.term()    
    return status, results

def resolve_server(server): 
    s = server
    if isinstance(s, str):   #only a node name 
        s= (s, )
    if len(s)==2: 
        host, port = s 
        tunnel, tunnel_server = False , None
    elif len(s)==3: 
        host, port, tunnel_server = s 
        tunnel = True 
    elif len(s)==1: 
        host, = s 
        port = None 
        tunnel, tunnel_server = False, None
    else: 
        raise 
    
    host = host if host is not None else '127.0.0.1'
    port = port if port is not None else 90900
    
    if 'node' in host: 
        tunnel = 1
        port = 9090 
        tunnel_server =  'zhihuali@211.86.151.102'
        

    temp = ['host', 'port', 'tunnel', 'tunnel_server']
    dic = locals()
    res= {a: dic[a] for a in temp}
    return res 

def conn_server(server, info=0): 
    """
        return the result of running the task *runnable* with the given 
        arguments.
        
        params: 
            host: e.g. '210.45.117.30' or 'qtg7501' if use the later should add ip hostname pair in /etc/hosts
            querry:  querry whether server available 
            querry_timeout: 
                我曾经试过用 stopit module 来给recv设置timeout, 但是没有成功，应该是涉及到背后线程没有关闭
                refer to https://github.com/zeromq/pyzmq/issues/132
    """
    server_info = resolve_server(server)
    host = server_info['host']
    port = server_info['port']
    tunnel = server_info['tunnel']
    tunnel_server = server_info['tunnel_server']
    
    if info>0: 
        print_vars(vars(),  ['server_info'])
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    url = 'tcp://{}:{}'.format(host, port)
    if not tunnel:  #one should either use connect or tunnel_connection, not both.
        socket.connect(url)
    else:
        #zmq.ssh.tunnel_connection(socket, url, "myuser@remote-server-ip")
        #issue:  似乎tunnel对port有限制，不能用90900这样5位数的的端口
        zmq.ssh.tunnel_connection(socket, url, tunnel_server)
        #print 'tunnel succeed: {}'.format(url)
    
    if 0: 
        socket.setsockopt(zmq.LINGER, 0)   #this is needed or else timeout wont work 
        #socket.send_pyobj('querry')    
        socket.send_pyobj({'header': 'querry'})    
        # use poll for timeouts:
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        if poller.poll(querry_timeout*1000): # 10s timeout in milliseconds
            server_status = socket.recv_pyobj()
        else:
            #raise IOError("Timeout processing auth request")        
            status = server_status = 'not_reachable'

        if 0:  # below not working  
            try: 
                with stopit.SignalTimeout(querry_timeout, False) as ctx:
                #with stopit.ThreadingTimeout(querry_timeout, False) as ctx:
                    print 'tttttry', port, host  
                    server_status = socket.recv_pyobj()
            except Exception as err: 
                print 'rrrraise', err 
                #socket.close()
                #context.term()
                #raise 
                #server_status = 'not_reachable'
                raise 
            print 'sssssss', ctx.state    
            if ctx.state == ctx.EXECUTED:
                pass # All's fine, everything was executed within 10 seconds
            elif ctx.state == ctx.EXECUTING:
                pass # Hmm, that's not possible outside the block
            elif ctx.state == ctx.TIMED_OUT:
                server_status = 'not recheable' # Eeek the 10 seconds timeout occurred while executing the block
            elif ctx.state == ctx.INTERRUPTED:
                pass 
                # Oh you raised specifically the TimeoutException in the block
            elif ctx.state == ctx.CANCELED:
                pass # Oh you called to_ctx_mgr.cancel() method within the block but it # executed till the end
            else:
                pass 
                # That's not possible            
            #print 'aaaaaafter ', ctx.state == ctx.TIMED_OUT , ctx.state == ctx.EXCUTING 
            print 'aaaaaafter ', ctx.state == ctx.TIMED_OUT , ctx.TIMED_OUT,  
    
    # these are not necessary, but still good practice:
    #socket.close()
    #context.term()    
    return context, socket 

def send_msg(msg, socket=None, server=None,  querry_timeout=5, info=0):
    """
        return the result of running the task *runnable* with the given 
        arguments.
        
        params: 
            host: e.g. '210.45.117.30' or 'qtg7501' if use the later should add ip hostname pair in /etc/hosts
            querry:  querry whether server available 
            querry_timeout: 
                我曾经试过用 stopit module 来给recv设置timeout, 但是没有成功，应该是涉及到背后线程没有关闭
                refer to https://github.com/zeromq/pyzmq/issues/132
    """
    if socket is None: 
        assert server is not None 
        context, socket = conn_server(server, info=info)
    
    if 1: 
        socket.setsockopt(zmq.LINGER, 0)   #this is needed or else timeout wont work 
        #socket.send_pyobj('querry')    
        socket.send_pyobj(msg)    
        # use poll for timeouts:
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        if poller.poll(querry_timeout*1000): # 10s timeout in milliseconds
            reply = socket.recv_pyobj()
        else:
            #raise IOError("Timeout processing auth request")        
            #reply = None
            reply = 'not_reachable'

        if 0:  # below not working  
            try: 
                with stopit.SignalTimeout(querry_timeout, False) as ctx:
                #with stopit.ThreadingTimeout(querry_timeout, False) as ctx:
                    print 'tttttry', port, host  
                    server_status = socket.recv_pyobj()
            except Exception as err: 
                print 'rrrraise', err 
                #socket.close()
                #context.term()
                #raise 
                #server_status = 'not_reachable'
                raise 
            print 'sssssss', ctx.state    
            if ctx.state == ctx.EXECUTED:
                pass # All's fine, everything was executed within 10 seconds
            elif ctx.state == ctx.EXECUTING:
                pass # Hmm, that's not possible outside the block
            elif ctx.state == ctx.TIMED_OUT:
                server_status = 'not recheable' # Eeek the 10 seconds timeout occurred while executing the block
            elif ctx.state == ctx.INTERRUPTED:
                pass 
                # Oh you raised specifically the TimeoutException in the block
            elif ctx.state == ctx.CANCELED:
                pass # Oh you called to_ctx_mgr.cancel() method within the block but it # executed till the end
            else:
                pass 
                # That's not possible            
            #print 'aaaaaafter ', ctx.state == ctx.TIMED_OUT , ctx.state == ctx.EXCUTING 
            print 'aaaaaafter ', ctx.state == ctx.TIMED_OUT , ctx.TIMED_OUT,  
    
    
    return reply 

class Client(object): 
    pass

def server_discover(servers=None, exclude_patterns=None, querry_timeout=5, qsize=8, info=0): 
    """
        issue: todo: use multiprocessing instead of thrading may faster, 
            qsize should be tuned to a better value 
    """
    
    #if servers is None: 
    if servers is None: 
        servers= []
    if servers== 'all': 
        servers= (
                ['node%d'%i for i in range(1, 100)]  #8cpu, normal, mem48, mem96
            +   ['localhost', 'qtg7501']
                )
    
    print('run server_discover ... ') 
    
    res = []
    msg = {'header': 'querry', 'what': 'is_exists'}

    if 0:  # serial  
        for s in servers: 
            rep=send_msg(msg, server=s, querry_timeout=querry_timeout, info=info)
            if rep == 'Y' : 
                res.append(s)
    else: 
        #qsize = 8   #this could not be too large, or else error may occor 
        from Queue import Queue 
        qq = Queue(qsize)
        def func(s): 
            try: 
                rep=send_msg(msg, server=s, querry_timeout=querry_timeout, info=info)
            except Exception as err: 
                rep = str(err)
                #rep = 'error'
            qq.put((s, rep))
        n = len(servers)/qsize  + 1 
        for i in range(n) : 
            tt = []
            for s in servers[i*qsize: i*qsize + qsize]: 
                t = threading.Thread(target=func, args=(s, ))
                t.start()
                tt.append(t)
                
            for t in tt: 
                t.join()
            temp = [qq.get() for a in range(qq.qsize())]
            #print_vars(vars(),  ['i, temp'])
            res.extend(temp)
        res = [r[0] for r in res if r[1] == 'Y' ]
    if exclude_patterns is not None: 
        for e in exclude_patterns: 
            res= [r for r in res if e not in r]
            
    print('servers found are:\n\t %s'%(res, )) 
    return res 

def stop_servers(servers): 
    msg = {'header': 'stop'}
    for s in servers: 
        print s, send_msg(msg=msg, server=s)

def run_one_in_queue(task, servers_cycle, info=0, querry=True, 
        querry_timeout=5, 
        try_period=None): 
    """
        params: 
            tasks:  
                e.g. [(func1, args1, kwargs1), ..., (funcn, argsn, kwargsn)]
            servers: 
                e.g. [(ip1, port1), (ip2, port2), ... ]
    """
    try_period = try_period if try_period is not None else 10  

    if 1: 
        status = 'run'
        while status != 'done' : 
            #s = servers[cycle.next()]
            s = servers_cycle.next()
            if 1: 
                s1 = resolve_server(s)
                host = s1['host']
                port = s1['port']
                tunnel = s1['tunnel']
                tunnel_server = s1['tunnel_server']
                
            status, res = queue(func, args=args, kwargs=kwargs,
                    querry_timeout=querry_timeout, 
                    querry=querry, host=host, port=port, 
                    tunnel=tunnel, tunnel_server=tunnel_server)
            if info>0: 
                print 'trying server {}: status={}, {}/{}'.format((host, port, tunnel_server), 
                        status, count+1, num_of_tasks)
            if status  == 'done' : 
                print 'distributed job {}/{} func {} to server {} with kwargs {}\n'.format(
                        count+1, num_of_tasks, 
                        func.__name__, (host, port, tunnel_server), kwargs)
            elif status == 'not_reachable': 
                #servers.remove(s)  #temporarily comment it 
                #servers_cycle = itertools.cycle(servers)
                #print 'removed {} from server list'.format(s)
                if len(servers)<1: 
                    raise 
            time.sleep(try_period)

def run_many(tasks, servers=None, info=0, querry=True, 
        querry_timeout=5, 
        try_period=None): 
    """
        params: 
            tasks:  
                e.g. [(func1, args1, kwargs1), ..., (funcn, argsn, kwargsn)]
            servers: 
                e.g. [(ip1, port1), (ip2, port2), ... ]
    """
    try_period = try_period if try_period is not None else 10  
    num_of_tasks = len(tasks)
    #if servers is None: 
    if servers == 'all': 
        print( '\n\nrun server_discover ... ')
        servers = server_discover('all', querry_timeout=1, info=1)
        print('servers found are %s'%(servers, ) ) 
        
    for count, t in enumerate(tasks):
        args, kwargs = tuple(), {}
        if len(t)==1: 
            func = t[0]
        elif len(t)==2: 
            #func, args= t
            func = t[0]
            t1 = t[1]
            if isinstance(t1, tuple): 
                args= t1
            elif isinstance(t1, dict): 
                kwargs= t1
            else: 
                raise 
        elif len(t)==3: 
            func, args, kwargs = t
        else: 
            raise 
        
        cycle = itertools.cycle(range(len(servers)))
        servers_cycle = itertools.cycle(servers)
        status = 'run'
        while status != 'done' : 
            #s = servers[cycle.next()]
            s = servers_cycle.next()
            if 1: 
                s1 = resolve_server(s)
                host = s1['host']
                port = s1['port']
                tunnel = s1['tunnel']
                tunnel_server = s1['tunnel_server']
                
            status, res = queue(func, args=args, kwargs=kwargs,
                    querry_timeout=querry_timeout, 
                    querry=querry, host=host, port=port, 
                    tunnel=tunnel, tunnel_server=tunnel_server)
            if info>0: 
                print 'trying server {}: status={}, {}/{}'.format((host, port, tunnel_server), 
                        status, count+1, num_of_tasks)
            if status  == 'done' : 
                print 'distributed job {}/{} func {} to server {} with kwargs {}\n'.format(
                        count+1, num_of_tasks, 
                        func.__name__, (host, port, tunnel_server), kwargs)
            elif status == 'not_reachable': 
                #servers.remove(s)  #temporarily comment it 
                #servers_cycle = itertools.cycle(servers)
                #print 'removed {} from server list'.format(s)
                if len(servers)<1: 
                    raise 
            time.sleep(try_period)

class TaskManager(object): 
    """
        分布式队列，但是又统一在这里，
        目的是为了监控优化队列时间分布：设置优先级等
    """
    TASK_DB = OrderedDict()
    def __init__(self, **kwargs):
        default_args = {
                'host': '127.0.0.1', 
                'port': 9099, 
                'info': 0, 
                'servers': [], 
                }
        
        for k in default_args: 
            if kwargs.has_key(k): 
                default_args[k] = kwargs[k]
        
        for k, v in default_args.iteritems(): 
            setattr(self, k, v)
    
    def start(self): 
        pass 
        self._context = zmq.Context()
        self.socket = self._context.socket(zmq.REP)
        self.socket.bind('tcp://{}:{}'.format(self.host, self.port))
        
        if 0: 
            while True:
                runnable_string, args, kwargs = self.socket.recv_pyobj()
                runnable = pickle.loads(runnable_string)
                    
                response = self._do_work(runnable, args, kwargs)
                self.socket.send_pyobj(response)
                
                if runnable.__name__ == 'stop' : 
                    break 
        
        # all share the same TASK_DB 
        while True: 
            pass 
        
    @staticmethod
    def send_tasks(tasks): 
        pass  
    
class TestIt(unittest.TestCase): 
    INITED = False 
    #def setUp(self): 
    @classmethod 
    def setUpClass(cls): 
        #here use 9090 while not 90900 because if use later 90900 the test of tunnel not working 
        server_list = [(None, 9090), (None, 90901), (None, 90902)] 
        cls.server_list = server_list 
        from config import config
        cfg = config.copy()
        cfg.update(info=1, start=1)
        
        cc = {}
        cc[0] = cfg.copy()
        cc[0].update(port=9090)
        cc[1] = cfg.copy()
        cc[1].update(port=90901)
        cc[2] = cfg.copy()
        cc[2].update(mode='with_return')
        cc[2].update(port=90902)
        
        
        if 1: 
            worker_proc = []
            #for s in server_list: 
            #    host, port = s 
            #    mode = 'ansync' if port != 90902  else 'with_return'
            #    proc = Process(target=Worker, args=(), 
            #            kwargs={'port': port, 
            #                'start': 1, 'info': 1, 'mode': mode}) 
            #    worker_proc.append(proc)
            #    proc.start() 
            for  i in range(3): 
                proc = Process(target=Worker, args=(), 
                        kwargs=cc[i]) 
                worker_proc.append(proc)
                proc.start() 
            
            cls.worker_proc = worker_proc 
        
    def test_basic(self): 
        pass 
        def test_func(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 3
        
        if 0:
            for i in range(5): 
                queue(test_func, args=(i, ), host=None, port=90908, 
                        tunnel=0, querry_timeout=1, querry=1)
    
    def test_parallel_queue_async(self): 
        def test_func(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 3
        
        #Process(target=queue, args=(func, )).start() 
        for i in range(10): 
            print queue(test_func, args=(i, ), port=self.server_list[1][1], querry=1)
    
    def test_with_return(self): 
        def test_func(i, info=0): 
            if info>0: 
                #print '#%s, pid=%d'%(i, os.getpid())
                pass 
            time.sleep(1)
            return i **2
        
        #Process(target=queue, args=(func, )).start() 
        for i in range(6): 
            res=queue(test_func, args=(i, 1), port=self.server_list[2][1], querry=0)
            print 'rrr', res
            self.assertEqual(res, ('done', test_func(i, 0))) 
   
    def test_run_many(self): 
        def f1(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 1
        def f2(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 2
        
        tasts = [(f1, (1, )), (f2, (2, )), (f2, (3, )),  (f1, (4, )), (f2, (5, ))]
        tasts = tasts*4  
        
        not_reachable = [('1.2.3.4', None)]
        run_many(tasts[: 3], not_reachable + self.server_list[: 1], 
                querry_timeout=0.5, 
                try_period=0.1)
        
        run_many(tasts, self.server_list, try_period=0.1)
   
    def test_tunnuel(self): 
        def test_func(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 3
        
        #Process(target=queue, args=(func, )).start() 
        for i in range(5): 
            print_vars(vars(),  ['i'])
            #print queue(test_func, args=(i, ), querry=1, tunnel=0, port=9090, tunnel_server='localhost')
            #print queue(test_func, args=(i, ), querry=0, tunnel=1, port=9090, tunnel_server='zhli@127.0.0.1:22')
            print queue(test_func, args=(i, ), querry=1, 
                    tunnel=1, host='node93', port=9090, 
                    querry_timeout= 0.1, 
                    tunnel_server= 'zhihuali@211.86.151.102')
            #print queue(test_func, args=(i, ), querry=0, tunnel=1, port=9090, tunnel_server='10.0.1.2')
    
    def xtest_tunnuel_2(self): 
        pass 
        def test_func(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 3
        for i in range(10): 
            print queue(test_func, args=(i, ), host='node96', port=9090, 
                    tunnel=1, tunnel_server='zhihuali@211.86.151.102',  querry=0)
            host = '210.45.121.30'
            #host = 'localhost'
            #print queue(test_func, args=(i, ), host=host, port=9090, 
            #        tunnel=1, tunnel_server='210.45.121.30',  querry=0)
   
    #def tearDown(self): 
    @classmethod
    def tearDownClass(cls): 
        print '\n ------------ tear down ---------------'
        def stop(*args, **kwargs): 
            pass
       
        #for i, s in enumerate(cls.server_list): 
        #    queue(stop, port=s[1], querry=1)
        #    cls.worker_proc[i].join()
        for i, s in enumerate(cls.server_list): 
            queue(None, kill_server=1,  port=s[1], querry=1)
            #cls.worker_proc[i].send_shut_down()
            cls.worker_proc[i].join()
    
    def test_server_discover(self): 
        #_, socket = conn_server(('localhost', 9090))
        msg = {'header': 'querry', 'what': 'is_exists'}
        print send_msg(msg=msg, server=('localhost', 9090))
        #ss = [('localhost', 90900), ('localhost', 90901), ('localhost', 90902)]
        #ss = ['localhost', 'node93', 'node98', 'node95', 'qtg7501']
        ss = ['localhost', 'qtg7501', 'node98', ('node93', 9090, 'zhihuali@211.86.151.102')]
        ss = ['node%d'%i for i in range(10)]
        #ss= None 
        res = server_discover(ss, info=-1, querry_timeout=0.3)

    def test_is_available(self): 
        def test_func(i, **kwargs): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 3
        
        kwargs = {'NUM_OF_THREADS': 20}
        for i in range(5): 
            res=queue(test_func, args=(i, ), kwargs=kwargs.copy(), 
                    host=None, port=9090, tunnel=0, querry_timeout=1, querry=1)
            print i, res 
            self.assertTrue('refuse' in res[0])

    def test_supervise(self): 
        def test_func(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 3

        from config import config
        cfg = config.copy()
        cfg.update(info=1, start=1, long_idle_shut_down=1, 
                idle_time_limit=2, check_idle_period=0.2)
        cc = cfg.copy()
        cc.update(port=9070)
        
        proc = Process(target=Worker, args=(), 
                kwargs=cc) 
        proc.start() 
        time.sleep(1)
        queue(test_func, args=(1, ), host=cfg['host'], port=9070, 
                tunnel=0,   querry=0)
            
        
    def test_temp(self): 
        pass 
        def test_func(i): 
            print '#%s, pid=%d'%(i, os.getpid())
            time.sleep(1)
            return 3
        #for i in range(10): 
        #    #print queue(test_func, args=(i, ), host='node96', port=9090, 
        #    #        tunnel=1, tunnel_server='zhihuali@211.86.151.102',  querry=0)
        #    host = '210.45.121.30'
        #    host = 'localhost'
        #    print queue(test_func, args=(i, ), host=host, port=90908, 
        #            tunnel=0, tunnel_server='localhost',  querry=0)
        
        #res = server_discover('all', info=-1, querry_timeout=1)
        
        if 0:
            for i in range(5): 
                res=queue(test_func, args=(i, ), host=None, port=9090, 
                        tunnel=0, querry_timeout=1, querry=1)
                print i, res 
        

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
        
        if 1:
            TestIt.test_temp=unittest.skip("skip test_temp")(TestIt.test_temp) 
            unittest.main()
            
        else: 
            suite = unittest.TestSuite()
            add_list = [
                #'test_basic', 
                #'test_parallel_queue_async', 
                #'test_with_return', 
                #'test_run_many', 
                
                #'test_tunnuel', 
                
                #'test_server_discover', 
                #'test_is_available', 
                'test_supervise', 
                #'test_temp', 
                
            ]
            for a in add_list: 
                suite.addTest(TestIt(a))

            unittest.TextTestRunner(verbosity=0).run(suite)
    
    
    
    
    
