#!/bin/python
"""
Program to illustrate multiprocess TCP echo server using Pyev module.

 - Supports multiprocess model where master process listens/accepts connections
   and worker processes handles all established client connections.

 - Master process distributes connected sockets to existing workers in a round robin fashion.

 - Socket handle/FD reduction and rebuilding is done across master to worker boundaries.

 - Depends on multiprocessing module for process launch and IPC Queue.

 - Depends on Pyev module ( http://code.google.com/p/pyev/ ) for event management.
 
 - Code initially based on Pyev's sample echo server.

"""

#This file is placed under the "New BSD License" just like Pyev itself.

#Copyright (c) 2013, Sriraam Vijayaraghavan
#Some rights reserved.

#    Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

#    Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

#    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

#    Neither the name of the <ORGANIZATION> nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

#    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import time
import multiprocessing
from multiprocessing.reduction import reduce_handle, rebuild_handle
import Queue
import os 
import socket
import signal
import errno
import logging
import sys
import pyev

try:
    import resource
#    resource.setrlimit(resource.RLIMIT_NOFILE, (655350, -1))
except ValueError , e:
    print '----->',e
    print 'must change /etc/security/limits.conf'
    pass 

logging.basicConfig(level=logging.ERROR)

STOPSIGNALS = (signal.SIGINT, signal.SIGTERM)
NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

# test big size to respones
body ='a' * 1600

# used ab -k -n100000 -c1000 http://127.0.0.1:5000/ for test, must add the Keep-Alive Header.
resp = 'HTTP/1.0 200 OK\r\nContent-Length: 1600\r\nConnection: Keep-Alive\r\n\r\n%s' %body
        
class Connection(object):
    """ Handles data IO for a single TCP connection """

    def __init__(self, sock, remote_address, loop, cnxn_id, parent):
        self.sock = sock
        self.remote_address = remote_address
        self.loop = loop
        self.cnxn_id = cnxn_id
        self.parent = parent

        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setblocking(0)
        self.sock.settimeout(0.5)

        self.buf = ""
        self.resp = resp

        self.watcher = pyev.Io(self.sock.fileno(), pyev.EV_READ, self.loop, self.io_cb)
        self.watcher.start()

        logging.debug("[{0}:{1}] Connection ready with [{2}]".format(self.parent.name,self.cnxn_id,self.remote_address))

    def reset(self, events):

        self.watcher.stop()
        self.watcher.set(self.sock, events)
        self.watcher.start()

    def handle_error(self, msg, level=logging.ERROR, exc_info=True):

#        logging.error("[{0}:{1}] Error on connection with [{2}]:[{3}]".format(self.parent.name,self.cnxn_id,msg,self.remote_address),
#                    exc_info=exc_info)
        self.close()

    def handle_read(self):

        buf = ""
        try:
            buf = self.sock.recv(2048)
        except Exception as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error reading from {0}".format(self.sock))
                return
            #else:
            #    return
        if len(buf):
#            logging.debug("[%s:%d] Received data: %s"%(self.parent.name,self.cnxn_id,buf))
            self.buf += buf
            self.reset(pyev.EV_READ | pyev.EV_WRITE)
        else:
            self.handle_error("Graceful connection closed by peer", logging.DEBUG, False)

    def handle_write(self):

        try:
            if self.buf:
                #sent = self.sock.send(self.buf)
                sent = self.sock.send(self.resp)
                #self.close()
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error writing to {0}".format(self.sock))
        else :
            self.reset(pyev.EV_READ)
            #self.buf = self.buf[sent:]
            #if not self.buf:
            #    self.reset(pyev.EV_READ)

    def io_cb(self, watcher, revents):

        #logging.debug("[%s:%d] io_cb called "%(self.parent.name,self.cnxn_id))
        if revents & pyev.EV_READ:
            self.handle_read()
        elif revents & pyev.EV_WRITE:
            self.handle_write()
        else:
            logging.debug("[%s:%d] io_cb called with unknown event %s"%(self.parent.name,self.cnxn_id,str(revents)))

    def close(self):

        self.sock.close()
        self.watcher.stop()
        self.watcher = None
        logging.error("[{0}:{1}] Connection closed with [{2}]".format(self.parent.name,self.cnxn_id,self.remote_address))


class ServerWorker(multiprocessing.Process):
    """ A worker process which 
            - handles incoming socket FDs from the master and recreates a socket from it,
            - creates and manages a bunch of connection objects ( one per connected socket )  
            - uses own pyev event loop to manage all the connected sockets
    """
    def __init__(self,name,in_q,out_q):
        multiprocessing.Process.__init__(self,group=None,name=name)
        self.in_q = in_q
        self.out_q = out_q
        self.loop = pyev.Loop(flags=pyev.EVBACKEND_EPOLL)
        self.watchers = []
        self.client_count = 0

        # Obtain the Queue object's underlying FD for use in the event loop
        self.in_q_fd = self.in_q._reader.fileno()

        self.watchers.append(pyev.Io(self.in_q_fd, 
                                     pyev.EV_READ, 
                                     self.loop,
                                     self.in_q_cb))

        #self.watchers.extend(pyev.Signal(sig, self.loop, self.signal_cb)
        #                 for sig in STOPSIGNALS])

        self.cnxns = {}

        logging.debug("ServerWorker[{0}:{1}]: Instantiated.".format(os.getpid(),self.name))

    def run(self):

        for watcher in self.watchers:
            watcher.start()

        logging.info("ServerWorker[{0}:{1}]: Running...".format(os.getpid(),self.name))
        self.loop.start()
        logging.info("ServerWorker[{0}:{1}]: Exited event loop!".format(os.getpid(),self.name))

    def stop(self):

        while self.watchers:
            self.watchers.pop().stop()

        self.loop.stop(pyev.EVBREAK_ALL)

        self.out_q.put("quitdone")

        logging.info("ServerWorker[{0}:{1}]: Stopped!".format(os.getpid(),self.name))

        sys.exit(0)

    def reset(self, events):

        self.watchers[0].stop()
        self.watchers[0].set(self.in_q_fd, events)
        self.watchers[0].start()

    def signal_cb(self, watcher, revents):

        self.stop()

    def in_q_cb(self, watcher, revents):

        try:
            val = self.in_q.get()
            #val = self.in_q.get(True,interval)
            logging.debug("ServerWorker[{0}:{1}]: Received inQ event!".format(os.getpid(),self.name))
            if type(val) == type((1,)):

                # Construct a proper socket object from the socket FD
                client_socket_handle,client_address = val
                client_fd = rebuild_handle(client_socket_handle)
                client_socket = socket.fromfd(client_fd, socket.AF_INET, socket.SOCK_STREAM)

                logging.debug("ServerWorker[{0}:{1}]: Adding connection [{2}] from [{3}].".format(os.getpid(),self.name,self.client_count,client_address))

                self.client_count += 1
                self.cnxns[client_address] = Connection(client_socket, client_address, self.loop, self.client_count, self)

                self.reset(pyev.EV_READ)

            elif type(val) == type("") and val == "quit":

                logging.info("ServerWorker[{0}:{1}]: Received quit message!".format(os.getpid(),self.name))
                self.stop()

        except Queue.Empty:
            # Timed-out, carry on
            pass
        


class ServerMaster(object):
    """ Master process which does the following:
          - Sets up nonblocking listening socket 
          - Launches bunch of worker processes and sets up IPC queues to manage them
          - Uses pyev default loop to manage events on the listen socket,signals and worker queues
          - On successful accept(), passes the connected socket to a chosen worker process
    """
    def __init__(self, 
            start_server_ip="127.0.0.1",
            start_server_port=5000,
            num_server_workers=1):

        self.start_server_ip = start_server_ip
        self.start_server_port = start_server_port
        self.num_server_workers = num_server_workers
        self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_sock.bind((start_server_ip,start_server_port))
        self.listen_sock.setblocking(0)
        self.listen_sock.settimeout(1)
        self.address = self.listen_sock.getsockname()

        self.worker_procs = []
        self.worker_queues = []

        for i in range(num_server_workers):

            # Create a pair of (inQ,outQ) for IPC with the worker
            worker_in_q = multiprocessing.Queue()
            worker_out_q = multiprocessing.Queue()

            self.worker_queues.append((worker_in_q,worker_out_q))

            # Create the worker process object
            worker_proc = ServerWorker("SW."+str(i+1), 
                                       worker_in_q,
                                       worker_out_q,
                                       )

            worker_proc.daemon = True
            self.worker_procs.append(worker_proc)
        
            # Start the worker process
            worker_proc.start()
    
        # By now the server workers have been spawned

        # Setup the default Pyev loop in the master 
        self.loop = pyev.default_loop(flags=pyev.EVBACKEND_EPOLL)

        # Prepare signal , out Q and connection watchers
        self.sig_watchers = [pyev.Signal(sig, self.loop, self.signal_cb)
                              for sig in STOPSIGNALS]

        self.q_watchers = [pyev.Io(fd=worker.out_q._reader.fileno(), 
                                  events=pyev.EV_READ,
                                  loop=self.loop, 
                                  callback=self.out_q_cb,
                                  data=worker)
                            for worker in self.worker_procs]

        self.socket_watchers = [pyev.Io(fd=self.listen_sock.fileno(), 
                                        events=pyev.EV_READ, 
                                        loop=self.loop,
                                        callback=self.io_cb)]
        self.next_worker = 0

    def start(self):

        for watcher in self.sig_watchers:
            watcher.start()
        for watcher in self.q_watchers:
            watcher.start()
        for watcher in self.socket_watchers:
            watcher.start()

        self.listen_sock.listen(socket.SOMAXCONN)

        logging.info("ServerMaster[{0}]: Started listening on [{1.address}]...".format(os.getpid(),self))

        self.loop.start()

    def stop(self):

        logging.info("ServerMaster[{0}]: Stop requested.".format(os.getpid()))

        for worker in self.worker_procs:
            worker.in_q.put("quit")

        while self.sig_watchers:
            self.sig_watchers.pop().stop()
        while self.q_watchers:
            self.q_watchers.pop().stop()
        while self.socket_watchers:
            self.socket_watchers.pop().stop()

        self.loop.stop(pyev.EVBREAK_ALL)

        self.listen_sock.close()

        for worker in self.worker_procs:
            worker.join()

        logging.info("ServerMaster[{0}]: Stopped!".format(os.getpid()))
     
    def handle_error(self, msg, level=logging.ERROR, exc_info=True):

        logging.log(level, "ServerMaster[{0}]: Error: {1}".format(os.getpid(), msg),
                    exc_info=exc_info)
        self.stop()

    def signal_cb(self, watcher, revents):

        logging.info("ServerMaster[{0}]: Signal triggered.".format(os.getpid()))
        self.stop()

    def io_cb(self, watcher, revents):

        try:

            while True: # Accept as much as possible
                try:
                    client_sock, client_address = self.listen_sock.accept()
                except socket.timeout as err:
                    break
                except socket.error as err:
                    if err.args[0] in NONBLOCKING: 
                        break
                    else:
                        raise
                else:
                    logging.debug("ServerMaster[{0}]: Accepted connection from [{1}].".format(os.getpid(),client_address))

                    # Forward the new client socket to a worker in a simple round robin fashion
                    self.worker_procs[self.next_worker].in_q.put((reduce_handle(client_sock.fileno()),client_address))
                    client_sock.close() # Close the socket on the master side
                    client_sock = None
        
                    self.next_worker += 1
                    if self.next_worker >= self.num_server_workers:
                        self.next_worker = 0

        except Exception:
            self.handle_error("Error accepting connection")

    def out_q_cb(self, watcher, revents):

        try:
            val = watcher.data.out_q.get()
            #val = self.in_q.get(True,interval)
            logging.debug("ServerMaster received outQ event from [%s] data [%s]"\
                    %(watcher.data.name,str(val))) 
            if type(val) == type((1,)):
                pass
            elif type(val) == type("") and val == "quitdone":
                logging.debug("ServerWorker [%s] has quit"\
                    %(watcher.data.name,)) 

        except Queue.Empty:
            # Timed-out, carry on
            pass


if __name__ == "__main__":

    server_master = ServerMaster(
                        start_server_ip="0.0.0.0",
                        start_server_port=5000,
                        num_server_workers=10)

    server_master.start()


