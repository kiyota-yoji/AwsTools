#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, urllib, paramiko, re, hashlib, time
from datetime import datetime
from dateutil.relativedelta import relativedelta

class HandlerException(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class HiveHandler():

    def __init__(self, start_dt, end_dt=None, file_path='/user/hive/warehouse/pagecounts'):
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect('192.168.212.174')
        self.sftp = self.ssh.open_sftp()
        self.file_path = file_path
        self.start_dt = start_dt
        if end_dt is None:
            now = datetime.utcnow()
            self.end_dt = datetime(now.year, now.month, now.day, now.hour)
        else:
            self.end_dt = end_dt

    def get_file_list(self):
        now = datetime.utcnow()
        end_dt = datetime(now.year, now.month, now.day, now.hour)
        
        result = []
        (stdin, stdout, stderr) = self.ssh.exec_command('find %s -type f' % self.file_path)
        p = re.compile('^%s/y=\d{4}/ym=\d{6}/ymd=\d{8}/h=\d{2}/pagecounts-(\d{8})-(\d{2})(\d{4})\.gz$' % self.file_path)
        for line in stdout.readlines():
            m = p.match(line.rstrip())
            if m is None: continue
            file_dt = datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H')
            if file_dt < self.start_dt: continue
            if file_dt > self.end_dt: continue
            file_name = 'pagecounts-%s-%s%s.gz' % m.groups()
            result.append((file_dt, file_name))

        return result

    def get_partition_list(self):
        command = "hive -e \"show partitions pagecounts;\""
        (stdin, stdout, stderr) = self.ssh.exec_command(command)
        result = []
        err_list = stderr.readlines()
        if not 'OK' in [x.rstrip() for x in err_list]:
            raise HandlerException('get_partition_list(): failed. ' + err_list[-1].rstrip())
        p = re.compile('^y=\d{4}/ym=\d{6}/ymd=(\d{8})/h=(\d{2})$')
        for line in stdout.readlines():
            line = line.rstrip()
            m = p.match(line)
            if m is None: raise HandlerException('get_partition_list(): The partition %s does not match the pattern.' % line)
            file_dt = datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H')
            if file_dt < self.start_dt: continue
            if file_dt > self.end_dt: continue
            result.append(file_dt)
        return result

    def upload(self, file_dt, file_name, tmp_file_name):
        remote_path = self.file_path + '/' + file_dt.strftime('y=%Y/ym=%Y%m/ymd=%Y%m%d/h=%H')
        self.ssh.exec_command('mkdir -p %s' % remote_path)
        time.sleep(1.0)
        self.sftp.put(tmp_file_name, '%s/%s' % (remote_path, file_name))

    def add_partition(self, file_dt):
        command = file_dt.strftime("hive -e \"ALTER TABLE pagecounts ADD PARTITION (y='%Y', ym='%Y%m', ymd='%Y%m%d', h='%H')\"")
        (stdin, stdout, stderr) = self.ssh.exec_command(command)
        for line in stderr.readlines():
            line = line.rstrip()
            if line == 'OK':
                print "add_partition succeeded"
                return
            elif line.startswith('FAILED:'):
                #raise HandlerException('add_partition(): failed. ' + line)
                print "add_partition failed. " + line
                return
        #raise HandlerException('add_partition(): failed. unknown messages.')
        print "add_partition failed. unknown messages."

    def sync_partition(self):
        file_list = self.get_file_list()
        partition_list = self.get_partition_list()

        file_dt_set = set([x[0] for x in file_list])
        partition_dt_set = set(partition_list)

        unreg_dt_set = file_dt_set - partition_dt_set
        for dt in sorted(unreg_dt_set):
            print 'registering partition for %s ...' % str(dt),
            self.add_partition(dt)
            print 'done.'

    def close(self):
        self.sftp.close()
        self.ssh.close()
        print "closed"
            

class PagecountsManager():

    def __init__(self, handler, start_dt, end_dt=None):
        self.start_dt = start_dt
        if end_dt is None:
            now = datetime.utcnow()
            self.end_dt = datetime(now.year, now.month, now.day, now.hour)
        else:
            self.end_dt = end_dt
        self.handler = handler

    def get_file_list(self):

        target_ym = datetime(self.start_dt.year, self.start_dt.month, 1)
        file_list = []
        while target_ym <= self.end_dt:
            result = self._get_month_file_list(target_ym, ignore_error=(target_ym == self.end_dt))
            file_list += result
            target_ym += relativedelta(months=1)
        return file_list
    
    def _get_month_file_list(self, target_ym, ignore_error=False):
        # ignore_error: 月の切り替えのタイミングでのエラー制御
        url = target_ym.strftime('http://dumps.wikimedia.org/other/pagecounts-raw/%Y/%Y-%m/md5sums.txt')
        res = urllib.urlopen(url)
        lines = res.readlines()
        result = []
        for line in lines:
            (md5sum, file_name) = line.rstrip().split('  ')
            file_name_split = file_name.split('-')
            if file_name_split[0] != 'pagecounts': continue
            file_url = target_ym.strftime('http://dumps.wikimedia.org/other/pagecounts-raw/%Y/%Y-%m/') + file_name
            (ymd, hour) = (file_name_split[1], file_name_split[2][0:2])
            file_dt = datetime.strptime(ymd + hour, '%Y%m%d%H')
            if file_dt < self.start_dt: continue
            result.append((file_dt, file_url, file_name, md5sum))
        return result

    def sync(self):
        site_file_list = self.get_file_list()
        local_file_list = self.handler.get_file_list()

        site_dt_set = set([x[0] for x in site_file_list])
        local_dt_set = set([x[0] for x in local_file_list])
        diff_dt_set = site_dt_set - local_dt_set

        for (file_dt, file_url, file_name, md5sum) in site_file_list:
            if not file_dt in diff_dt_set: continue
            print "sync %s..." % file_dt
            tmp_file_name = self.download(file_url, md5sum)
            print 'tmp_file_name = %s ' % tmp_file_name

            self.handler.upload(file_dt, file_name, tmp_file_name)
            self.handler.add_partition(file_dt)

    def download(self, file_url, md5sum):
        (tmp_file_name, headers) = urllib.urlretrieve(file_url)
        f = open(tmp_file_name, 'rb')
        tmp_md5sum = self.md5_for_file(f)
        f.close()
        if tmp_md5sum != md5sum: raise HandlerException('download(%s): md5sum does not match!' % file_url)
        return tmp_file_name

    def md5_for_file(self, f, block_size=2**20):
        md5 = hashlib.md5()
        while True:
            data = f.read(block_size)
            if not data:
                break
            md5.update(data)
        return md5.hexdigest()

    def close(self):
        self.handler.close()
        

if __name__ == "__main__":

    start_dt = datetime(2014, 6, 1)
    end_dt = datetime(2014, 6, 30)
    manager = PagecountsManager(HiveHandler(start_dt, end_dt=end_dt), start_dt, end_dt=end_dt)
    #file_list = manager.get_file_list()
    #for (file_dt, file_url, file_name, md5sum) in file_list:
    #    print file_dt

    manager.sync()
    manager.close()

    #handler = HiveHandler(start_dt)
    #handler.get_partition_list()
    #handler.sync_partition()
