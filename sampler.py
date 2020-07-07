# /usr/bin/env python
# coding=utf-8
import os
import sys
import psutil
import subprocess
import time
import json
import threading
import socket
import struct

import logging
from logging import handlers

import re
import ConfigParser
import signal
from signal import SIGTERM
from time import sleep


from datetime import datetime,timedelta
try:
    import stagedb
except ImportError:
    print 'Import stagedb Error'


DEPLOY_DIR='/opt/sampler/'
DEFAULT_PROFILE = 'config.ini'

DEFAULT_REPORTING_HOST = '127.0.0.1'
DEFAULT_REPORTING_PORT = 1307

#gather Volume threshold limit value
DEFAULT_VOL_TLV = int(1e8)
DEFAULT_CPU_TLV = 8
DEFAULT_MEM_TLV = int(1e6)

#frequency sec.
DEFAULT_GATHER_FREQ = 1.0

DEFAULT_LOG_LEVEL = 'WARN'
DEFAULT_LOG_COUNT = 2
DEFAULT_LOG_SIZE = int(5.12e6) # ~=5MB
DEFAULT_LOG_FILE = 'sampler.log'

DEFAULT_EXPIRATION = 7.0  #days
DEFAULT_LIMITED = int(3e8)    # 300MB


def handler(sig,frame):
    print "\nYou Pressed Ctrl-C."
    sys.exit(sig)


signal.signal(signal.SIGINT, handler)

def ip2long(ip):
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]


def long2ip(ip):
    return socket.inet_ntoa(struct.pack("!L", ip))


def humanite(val):  # byte
    tag = 'B KB MB GB TB PB'.split()
    q, step = 0, 1e3
    while val > step:
        val = val / step
        q += 1
    return '%.1f' % val + tag[q]


class Gatherer(object):
    def __init__(self,handle):
        super(Gatherer, self).__init__()
        self.logger=handle
        self.rec=re.compile(r'[^\w]')

    def cpu(self):
        try:
            temp = psutil.sensors_temperatures()
            temp = temp['coretemp']
        except Exception, e:
            cpus={}
            try:
                for p in temp:
                    for i,t in enumerate(temp[p]):
                        cpus['%s%02d'%(p,i)]=t.current
                return {'Physical': cpus}
            except:
                self.logger.info(str(e))
        else:
            i,cpus,core =0, {},{}
            try:
                for t in temp:
                    if t.label.find('Physical') != -1:
                        i = int(t.label[-2:])
                        cpus['%02d' % i]=t.current
                    elif t.label.find('Core') != -1:
                        j = int(t.label[-2:])
                        c = '%02dp%02d' % (i, j)
                        core[c]=t.current
            except  Exception, e:
                self.logger.info(str(e))
            return {'Physical': cpus, 'Core': core}

    def mem(self,human=False):   #memory
        try:
            m = psutil.virtual_memory()
            flt = 'slab buffers inactive active percent'.split()
            dat = {str(k): m.__dict__[k] for k in m.__dict__ if k not in flt}
            if human:
                return {str(k): humanite(m.__dict__[k]) for k in m.__dict__ if k != 'percent'}
            return dat
        except Exception, e:
            self.logger.info(str(e))

    def vol(self,human=False):   #disk volume
        all={}
        try:
            for disk in psutil.disk_partitions():
                d=re.sub(self.rec,'',disk.mountpoint.replace('/','_'))
                p = psutil.disk_usage(disk.mountpoint)
                if human:
                    dat = {str(k): humanite(p.__dict__[k]) for k in p.__dict__ if k != 'percent'}
                    dat['use'] = p.percent
                else:
                    dat = {str(k): p.__dict__[k] for k in p.__dict__}
                all[d]=dat
        except Exception, e:
            self.logger.info(str(e))

        return all

    def io(self):
        try:
            d = psutil.disk_io_counters()
            return {'read_bytes': humanite(d.read_bytes), 'write_bytes': humanite(d.write_bytes)}
        except Exception, e:
            self.logger.info(str(e))

    def process(self, pid):
        try:
            p = psutil.Process(pid)
            lne=p.cmdline()
            cmd=p.name()
            if lne:
                cmd=' '.join(lne)
            return {'user': p.username(), 'pid': pid, 'start': p.create_time(), 'command': cmd}
        except Exception as e:
            self.logger.info(str(e))

    def hardware(self):
        try:
            popen = subprocess.Popen(('dmesg',),
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE,
                     bufsize=0)

            while popen.poll() is None:
                r = popen.stdout.readline()
                self.logger.info(r)

            if popen.poll() != 0:
                err = popen.stderr.read()
                self.logger.info(err)

        except Exception, e:
            self.logger.info(str(e))

    def ver(self):
        pipe = subprocess.Popen(('head','-n', '1','/etc/issue',),stdout = subprocess.PIPE).stdout
        os=pipe.readline()
        return os

    def pids(self):
        try:
            return psutil.pids()
        except:
            return []


class Sampler(object):
    config = {
        'cpu': DEFAULT_CPU_TLV,
        'mem': DEFAULT_MEM_TLV,
        'vol': DEFAULT_VOL_TLV,
        'host': DEFAULT_REPORTING_HOST,
        'port': DEFAULT_REPORTING_PORT,
        'sampling_frequency': DEFAULT_GATHER_FREQ,
        'storage_limits': DEFAULT_LIMITED,
        'expiration_days': DEFAULT_EXPIRATION,
        'clean_interval_hours' : 1,
        'logging_level':DEFAULT_LOG_LEVEL
    }
    def __init__(self):
        super(Sampler, self).__init__()
        self.runing=True
        self.Ref={'cpu':None,'mem':None,'vol':None}
        self.psi={}
        self.lock = threading.Lock()
        self.G=None
        self.Z=0
        self.cli=None
        self.pos=None
        self.way=''
        self.cche={}
        self.ready=False
        self.tag=''
        self.logger = logging.getLogger('sampler')
        self.profile()
        self.initialize()


    def profile(self):
        f=os.path.abspath(DEFAULT_PROFILE)
        if not os.path.isfile(f):
            f=DEPLOY_DIR+DEFAULT_PROFILE
        parser = ConfigParser.SafeConfigParser()
        r=parser.read(f)
        if r:
            if parser.has_section('config'):
                cfg=dict(parser.items('config'))
                for c in cfg:
                    if c in self.config:
                        if isinstance(self.config[c],str):
                            if c=='host':
                                try:
                                    long2ip(self.config.get('host'))
                                    self.config[c] = cfg[c]
                                except:
                                    self.config[c]=DEFAULT_REPORTING_HOST
                            elif c=='logging_level':
                                level = cfg[c].upper()
                                if level in ['DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR']:
                                    self.config[c] = level
                            else:
                                self.config[c] = cfg[c]
                        else:
                            try:
                                j=float(cfg.get(c))
                                if isinstance(self.config[c],int):
                                    j=int(j)
                                self.config[c] =j
                            except:
                                pass


        self.logger.setLevel(getattr(logging, self.config.get('logging_level')))


    def initialize(self):
        format = logging.Formatter('%(asctime)s %(message)s')
        handler = handlers.RotatingFileHandler(DEFAULT_LOG_FILE, 'a', DEFAULT_LOG_SIZE, DEFAULT_LOG_COUNT)
        handler.setLevel(logging.INFO)
        handler.setFormatter(format)
        self.logger.addHandler(handler)
        self.logger.info('Initialize the sampler')
        self.G = Gatherer(self.logger)


    def processd(self, ids):
        for pid in ids:
            try:
                pro = self.G.process(pid)
                tme = int(pro.get("start"))
            except: # psutil.NoSuchProcess:
                pass
            else:
                self.psi[str(pid)] = tme
                key='/sys|pss|%s|%d_%06d' % (self.pos, tme, pid)
                self.cche[key]= pro


    def process(self):  #self.psi is not empty
        x = set(self.G.pids())
        a = set(map(int, self.psi))

        z = x-a
        d = a-x
        self.processd(z)

        tms =time.time()
        for pid in d:
            key='/sys|psx|%s|%d_%06d' % (self.pos,self.psi[str(pid)],pid)
            self.cche[key]=tms

        if d:
            l=list(d)
            self.psi={k:self.psi[k] for k in self.psi if int(k) not in l}


    def test_cpu(self):
        cpu = self.G.cpu()
        d = self.Ref['cpu']
        tlv=self.config.get('cpu')
        over=False
        try:
            if cpu['Physical']:
                for k in cpu['Physical']:
                    if abs(cpu['Physical'][k] - d['Physical'][k]) >= tlv:
                        over=True
                        break
            else:
                for k in cpu['Core']:
                    if abs(cpu['Core'][k] - d['Core'][k]) >= tlv:
                        over=True
                        break
        except:
            over=True

        if over:
            return cpu


    def test_mem(self):
        try:
            mem = self.G.mem()
            d = self.Ref['mem']
            tlv=self.config.get('mem')
            over = False
            key = '/sys|mem|%s' % time.strftime('%Y%m%d%H%M%S')
            if abs(mem['free']-d['free']) >= tlv:
                over = True
        except:
            over = True

        if over:
            return mem


    def test_vol(self):
        try:
            vol = self.G.vol()
            d = self.Ref['vol']
            tlv=self.config.get('vol')
            over= False
            key = '/sys|vol|%s' % time.strftime('%Y%m%d%H%M%S')
            for k in vol:
                if abs(vol[k]['free']-d[k]['free'])>=tlv:
                    over=True
                    break
        except:
            over = True

        if over:
            return vol


    def test_dio(self):
        pass


    def gauge(self):
        for na in 'cpu mem vol'.split(' '):
            func ='test_%s'%na
            if hasattr(self, func):
                try:
                    val = getattr(self,func)()
                    if val:
                        tms =time.strftime('%Y%m%d%H%M%S')
                        key = '/sys|%s|%s' % (na,tms)
                        self.cche[key]=val
                        self.Ref[na]=val
                except Exception,e:
                    self.logger.info(str(e))


    def report(self):
        try:
            num=0
            for k in self.cche:
                v=json.dumps(self.cche[k])
                ok,_=self.async_put(k,v)
                if ok:
                    num+=len(k)+len(v)
                else:
                    raise RuntimeError()
            self.cche.clear()
        except:
            self.Z=0
            self.ready=False
            self.cche.clear()
        else:
            self.Z+=num


    def logs(self):
        try:
            for k in self.cche:
                v=json.dumps(self.cche[k])
                self.logger.warn(str((k, v)))
            self.cche.clear()
        except Exception, e:
            self.logger.error(k+'>>'+str(e))


    def reference(self):
        for to in 'cpu mem vol'.split(' '):
            try:
                val = self.G.__getattribute__(to)()
                self.Ref[to] = val
                key = '/sys|%s|%s' % (to, time.strftime('%Y%m%d%H%M%S'))
                self.cche[key]=val
            except:
                self.Ref[to] = 0

        self.processd(self.G.pids())
        self.way = 'log'


    def mirror(self):
        try:
            pss, psx = [], []
            for k, v in self.cli.keys('/sys|pss|%d' % self.pos):
                pss.append(k)

            for k, v in self.cli.keys('/sys|psx|%d' % self.pos):
                psx.append(k)

            if pss:
                if psx:
                    pss = set(pss) - set(psx)

                for k in pss:
                    i = k.find('_')  # key like 1551331340_032186
                    if i != -1:
                        p = str(int(k[i + 1:]))
                        self.psi[p] = int(k[:i])

            for to in 'cpu mem vol'.split(' '):
                for k, v in self.cli.keys_rev("/sys|%s" % to):
                    val = json.loads(v)
                    self.Ref[to] = val
                    break

            self.way = 'sdb'
        except Exception,e:
            self.ready=False


    def gather(self):
        print self.config
        host = self.config.get('host')
        port = self.config.get('port')
        try:
            self.cli= stagedb.client(host, port, 1)
            self.logger.info('Connect to report server ( %s : %d)' % (host,port))
        except:
            self.logger.info('Stagedb client: Initialization failed')
        try:
            p = psutil.Process(1)
            self.pos = int(p.create_time())
            self.logger.info('systemd process start time: %d'%self.pos)
        except Exception, e:
            self.pos = int(time.time())
            self.logger.info(str(e)+' Set mirror point： %d'%self.pos)

        #seq='cpu mem vol pss'.split()
        seq = []
        freq=self.config.get('sampling_frequency')
        size=self.config.get('storage_limits')
        exp=self.config.get('expiration_days')
        h=self.config.get('clean_interval_hours')
        ite = 3600 * h
        cir=int(time.time())+ite
        self.logger.info('Start collecting information by sampler')

        print 'self.ready',self.ready
        while True:
            try:
                if not self.ready:
                    try:
                        self.ready, _ = self.async_get('/ver')
                        print 'ready',self.ready
                    except:
                        self.ready=False

                if self.way=='':
                    if self.ready:
                        self.mirror()
                    else:
                        self.reference()
                elif self.ready and self.way=='log':
                    self.way = ''
                    continue
                elif not self.ready and self.way=='sdb':
                    self.reference()
                else:
                    self.gauge()
                    self.process()

                if self.way=='log':
                    self.logs()
                else:
                    self.report()

                tms=int(time.time())
                if self.ready:
                    if tms > cir:
                        if self.Z > size:
                            seq = 'cpu mem vol pss'.split()
                            cir = tms + ite
                            self.Z=0
                            print 'Start cleaning'
                            self.logger.info('Start cleaning')
                        else:
                            cir = tms + ite
                            self.logger.info('data size is less than the limit,reset timer')
                            print 'data size is less than the limit,reset timer'
                    elif len(seq)>= 1:
                        try:
                            tag = seq.pop()
                            self.Z += self.lessen(tag,exp)
                            self.logger.info('lessen %s record'%tag)
                            print 'lessen %s record'%tag
                        except Exception,e:
                            self.logger.info(str(e))
                            seq.append(tag)
                    else:
                        print 'size',self.Z
                else:
                    cir=tms+ite

            except Exception, e:
                try:
                    self.logger.info(str(e))
                except:
                    self.logger.info('Special string')

            sleep(freq)


    def lessen(self,tag,e):
        exp=timedelta(days=abs(e))
        day = datetime.now() - exp
        if tag=='pss':
            return self.less(day)
        tms = int(day.strftime('%Y%m%d%H%M%S'))
        arr,n = {},0
        for k, v in self.cli.keys('/sys|%s'%tag):
            if int(k) < tms:
                key='/sys|%s|%s' % (tag, k)
                arr[key]=len(v)
            n+=len(v)

        for k in arr:
            r=self.async_del(k)
        n-=sum(arr.values())
        return n


    def less(self,day):
        tms = int(time.mktime(day.timetuple()))
        head,tail=[],[]
        for pos in self.cli.subpath('/sys|pss'):
            if int(pos)<tms:
                head.append(int(pos))
            else:
                tail.append(int(pos))

        if head:
            pos=head.pop()
            tail.append(pos)

        for p in head:
            arr=[]
            for k, v in self.cli.keys('/sys|pss|%s' % p):
                arr.append('/sys|pss|%s|%s' % (p,k))

            for k, v in self.cli.keys('/sys|psx|%s' % p):
                arr.append('/sys|psx|%s|%s' % (p,k))

            for k in arr:
                r = self.async_del(k)


        bts=0
        if tail:
            for p in tail:
                for k, v in self.cli.keys('/sys|pss|%s' % p):
                    bts+=len(k)+len(v)
                for k, v in self.cli.keys('/sys|psx|%s' % p):
                    bts+=len(k)+len(v)

        arr,n= [],0
        for k, v in self.cli.keys('/sys|psx|%s' % pos):
            if int(float(v)) < tms:
                arr.append('/sys|psx|%s|%s' % (pos, k))
                n+=len(k)+len(v)
                key='/sys|pss|%s|%s'%(pos, k)
                val=self.cli.get(key)
                if val:
                    arr.append(key)
                    n += len(k) + len(val)
        for k in arr:
            r = self.async_del(k)

        if bts>0:
            bts -= n
        return bts


    def async_get(self,key):
        ret = []
        evt = threading.Event()

        def on_message(r, c):
            ret.append(True)
            if r.succ():
                ret.append(r.value())
            else:
                ret.append(None)
            evt.set()

        def on_failed(r, c):
            ret.append(False)
            ret.append(None)
            evt.set()

        self.cli.async_get(key, on_message, on_failed, None)
        evt.wait()
        return ret


    def async_put(self, key,val):
        ret = []
        evt = threading.Event()
        def on_message(r, c):
            ret.append(True)
            ret.append(r.succ())
            evt.set()

        def on_failed(r, c):
            ret.append(False)
            ret.append(None)
            evt.set()

        r=self.cli.async_put(str(key), str(val), on_message, on_failed, None)
        evt.wait()
        return ret


    def async_del(self,key):
        def on_message(r,c):
            return r

        def on_failed(r,c):
            return r
        self.cli.async_del(key,on_message,on_failed,None)


if __name__ == "__main__":
    sample = Sampler()
    sample.gather()