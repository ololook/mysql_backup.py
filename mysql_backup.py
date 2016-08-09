#!/bin/python
from datetime import datetime
import ConfigParser
import time,datetime
import sys, os, subprocess, tarfile
import socket
import MySQLdb
import re
import shlex
import string

username = "username"
password = "password"
hostname = "127.0.0.1"
backupdir="/backupdir"
bktype="LG"  #LG or PY
num=50

def GETIP():
    lcip=socket.gethostbyname(socket.gethostname())
    return lcip

def MKDIR(backupdir):
    if not os.path.exists(backupdir):
       os.makedirs(backupdir)

def RSYNC_BACKUP(backupset):
    cmd="`which rsync` -vzrtopg --progress %s  remotehost::mysqldump &>/tmp/rsynclog"
    os.popen(cmd % (backupset))

def info_recoder(sql):
        conn = MySQLdb.connect(host   = 'remotehost' ,
                               user   = 'gmonitor'      ,
                               passwd = 'gmonitor'      ,
                               db     = 'gmonitor'      ,
                               port   = 7307 )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
def error_info(host,port,info):
        info_recoder('insert into backup_except_info(ip,port,dbtype,st,info) \
                        values(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')'
                       %( host,
                          port,
                         'MySQL',
                          time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),
                          info)
                     )


def back_info(host,port,stime,etime,fsize,day,backnm,dbtype,backtype):
        info_recoder('insert into backupset(ip,port,st,ed,size,dat,nm,dbtype,backtype) \
                        values(\'%s\',\'%s\',\'%s\',\'%s\',\'%d\',\'%s\',\'%s\',\'%s\',\'%s\')'
                         %(host,port,stime,etime,fsize,day,backnm,dbtype,backtype)) 

def sstime():
          s_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
          return s_time

def mmtime():
          m_time=time.strftime('%Y-%m-%d',time.localtime(time.time()))
          return m_time

class  BACKUP_MYDB ():
       def __init__(self,host,port,user,passwd,backupdir):
          self.host         =host
          self.port         =port
          self.user         =user
          self.passwd       =passwd
          self.backupdir    =backupdir
           
       def mydump (self):
           
           cmd="`which mysqldump` --user=%s  --password=%s  --host=%s --port=%s \
                                  --default-character-set=utf8 \
                                  --hex-blob \
                                  --opt --routines \
                                  --triggers --single-transaction \
                                  --master-data=2 --all-databases  |gzip >%s%s_%s_MySQL_%s.sql.gz"
           try:
             s_time=sstime()
             iptime=mmtime()
             ipaddr=GETIP()
             os.popen(cmd %(self.user,self.passwd,self.host,self.port,backupdir,iptime,ipaddr,self.port))
             e_time=sstime()
             
             filesize =os.stat(backupdir+iptime+'_'+ipaddr+'_'+'MySQL'+'_'+self.port+'.'+'sql.gz').st_size
             print os.stat(backupdir+iptime+'_'+ipaddr+'_'+'MySQL'+'_'+self.port+'.'+'sql.gz').st_size
             filename =(iptime+'_'+ipaddr+'_'+'MySQL'+'_'+self.port+'.'+'sql.gz')
             back_info(ipaddr,self.port,s_time,e_time,filesize,iptime,filename,"MySQL","mysqldump")                  
           except Exception,ex:  
             error_info(ipaddr,self.port,"mysqldump failed")
             print Exception,":",ex  
           
       def innodump(self):
          s_time=sstime()
          cmd="`which innobackupex` --user=%s --password=%s --host=%s --port=%s \
                                          --slave-info --safe-slave-backup \
                                          --no-lock  --stream=tar /tmp/ \
                                          2>/tmp/innobackup.log | gzip >%s%s_%s_MySQL_%s.tar"
          e_time=sstime()
          try:
            ipaddr=GETIP()
            iptime=mmtime()
            s_time=sstime()
            os.popen(cmd %(self.user,self.passwd,self.host,self.port,backupdir,iptime,ipaddr,self.port))
            e_time=sstime()
            filesize =os.stat(backupdir+iptime+'_'+ipaddr+'_'+'MySQL'+'_'+self.port+'.'+'tar').st_size
            filename =(iptime+'_'+ipaddr+'_'+'MySQL'+'_'+self.port+'.'+'tar')
            back_info(ipaddr,self.port,s_time,e_time,filesize,iptime,filename,"MySQL","xtrabackup")
          except Exception,ex:
             error_info(ipaddr,self.port,"xtrabackup failed")
             print Exception,":",ex
          
def GETCNF():
    
    cnt=0
    mycnf = list([0*16]) 
    P1 = subprocess.Popen("ps axw | grep mysqld_safe",
                             shell=True,
                             stdout=subprocess.PIPE,
                           )
    
    std = P1.communicate()[0].split('\n')
    if  std !="" and re.search('cnf',str(std)):
      for line in std:
          line = line.strip('\n')
          line = line.strip('\'')
          line = line.strip( ) 
          if line !="" and re.search('cnf',str(line)):
                line=line.split('=')
                for myline in line:
                    if myline !="" and re.search('cnf',str(myline)):
                        print 'ok'
                cnt=cnt+1
                mycnf[cnt]=myline
                mycnf.remove(0) 
      return mycnf

def GETPORT():
    config = ConfigParser.ConfigParser()
    myfile=GETCNF()
    newList = [x for x in myfile if not x == 0]
    print newList
    cnt=0
    myport = list([0]*12)
    for my1 in newList:
        try:
          config.read(my1)
          v_port = config.get('client','port')
          myport[cnt]=v_port
        except ConfigParser.ParsingError:
          v_port = config.get('client','port')
          myport[cnt]=v_port
        cnt=cnt+1
    myport_new= [x for x in myport if not x == 0]
    print myport_new
    return myport_new

def BACKUP_INSTANCE():
   global role
   global read
   role='mysql'
   read=7
   backup_port=GETPORT()
   for instance in backup_port:
        if type(instance) is str:
            vvv=instance
        else:
            vvv=instance[0]
        try:
                db = MySQLdb.connect (  host   = hostname,
                                        user   = username,
                                        passwd = password,
                                        port   = int(vvv))
        except MySQLdb.Error, e:
                print "Error %d: %s" % (e.args[0], e.args[1])
                sys.exit(1)

        cur1 = db.cursor()
        cur2 = db.cursor()
        cur3 = db.cursor()
        sql1 ="show processlist"
        sql2 ="show variables like 'read_only%'"
        cur1.execute(sql1)
        cur2.execute(sql2)
        row2=cur2.fetchone()
        numrows = int(cur1.rowcount)
        print numrows 
        for x in range(0,numrows):
            result1 = cur1.fetchone()
            if re.search('system user',str(result1)):
                role='slave' 
                if row2[1]=="OFF":
                   role='master'
                
            elif numrows < num :
                 role='slave'  
        print role
        if role=='slave' and  bktype=="LG":
           bkmy=BACKUP_MYDB(hostname,vvv,username,password,backupdir)
           bkmy.mydump()

        elif role=="slave" and bktype=="PY":
           bkmy=BACKUP_MYDB(hostname,vvv,username,password,backupdir)
           bkmy.innodump()
                    
        else:
           print "role is master no need backup"

def DELETE_OVERDU_BACKUP():
  
    filelist=[]
    filelist=os.listdir(backupdir)
    for i in  filelist:
        print backupdir+i
        backupset=backupdir+i

        ftl =time.strftime('%Y-%m-%d',time.gmtime(os.stat(backupdir+i).st_mtime))
        year,month,day=ftl.split('-')
        ftll=datetime.datetime(int(year),int(month),int(day))
        year,month,day=time.strftime('%Y-%m-%d',time.gmtime()).split('-')
        localtll=datetime.datetime(int(year),int(month),int(day))
        days=(localtll-ftll).days
        print days
        if days==0:


           RSYNC_BACKUP(backupdir+i) 

        elif days>=7:
           try:
                os.remove(backupdir+i)
           except:
                print "delete overdue backup failed"
        else:
           print "ok"

def  main():

    BACKUP_INSTANCE()
    time.sleep(10)
    DELETE_OVERDU_BACKUP()

if __name__ == '__main__':
    main()