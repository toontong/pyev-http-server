pyev-http-server
================

Base on libev, pyev0.8.1-4.04, python2.6

# ab测试性能：
  ab -k -n1000000 -c1000 http://10.20.188.114:5000/
  2核的单CPU，2G内存，跨机器跑ab。起10个进程。
  
    Keep-Alive requests:    100000
    Total transferred:      166586580 bytes
    HTML transferred:       160083200 bytes
    Requests per second:    25062.64 [#/sec] (mean)
    
    
# 主要是要把handle_error, handle_read,handle_write 函数中的logging注释。RPS性能就能上到2.5w~3w.
