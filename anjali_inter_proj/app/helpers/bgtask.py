import asyncio
import datetime
import importlib
import threading
import dbclass as DB
from fastapi import HTTPException


class Backtask():
    __taskcount=0

    def __init__(self,taskname,projcode,createdby,modulename):
        self.__status="WAITING"
        self.__taskname=taskname
        self.__taskid=0
        self.__projcode=projcode
        self.__createdby=createdby
        self.__modulename=modulename
        Backtask.__taskcount+=1


    def __createconn__(self):
        conndetails={
            'db_type':config.DB['db_type'],
            'driver':config.DB['driver'],
            'username': config.DB['username'],
            'host': config.DB['host'],
            'password': config.DB['password'],
            'port': config.DB['port'],
            'sid': config.DB['sid']

        }
        self.db=DB(conndetails)

    def __get_conn__(self):
        return self.db

    def __gettime__(self):
        return datetime.datetime.now()

    def __createtask__(self):
        Backtask.__createconn__(self)
        db=Backtask.__createconn__(self)
        self.__creationtime= Backtask.__gettime__()
        info=db.info()
        dbtype=info['db_type']
        if(dbtype=='oracle'):
            query=f"INSERT INTO background_task (seq_num, task_name, proj_code, module_name, task_status, task_creation_time)values(bg_task_seq.NEXTVAL,'{self.__taskname}','{self.__projcode}','{self.__modulename}','WAITING','')"
            query1="select bg_task_seq.currval from dual"
        elif(dbtype=='postgresql'):
            query=f"INSERT INTO background_task (task_name, proj_code, module_name, task_status, task_creation_time)VALUES ('{self.__taskname}','{self.__projcode}','{self.__modulename}', 'WAITING','');"
            query1="select currval('bg_task_seq')"
        db.exec_query(query)
        seqnum=db.get_one_data(query1)
        self.__taskid=seqnum[0]
        print("\n\n--> Task created. Task id is:",self.__taskid)

    def updatestatus(self,status):
        allowedvals=['RUNNING','WAITING','FAILED','FINISHED']
        if status in allowedvals:
            print("INFO: TASK "+str(self.__taskid)+" status changed to "+status)
            if(status=="RUNNING"):
                print("\n\nTask is in running state")
                self.__starttime=Backtask.__gettime__()
                db=Backtask.__get_conn__(self)
                query="UPDATE BACKGROUND_TASK set task_start_time= '"+str(self.__starttime)+"' where seq_num='"+str(self.__taskid)+"'"
                db.exec_query(query)
            db=Backtask.__get_conn__()
            query="update background_task set task_status ='"+status+"' where seq_num='"+str(self.__taskid)+"'"
            db.exec_query(query)
            self.__status=status
        else:
            raise HTTPException(status_code=400,detail={"error":"Invalid status passed"})

    def getstatus(self):
        return self.__status

    def get_num_task_in_exec(self):
        return self.__taskcount

    def __end__(self):
        self.__completiontime=Backtask.__gettime__()
        db=Backtask.__get_conn__()
        datetimeformat='%Y-%m-%d %H:%M:%S.%f'
        self.__duration=datetime.datetime.strptime(str(self.__completiontime),datetimeformat)- datetime.datetime.strptime(str(self.__starttime),datetimeformat)
        print('total duration for task- '+str(self.__taskid)+'is : '+str(self.__duration))
        query="Update background_task set task_end_time ='"+str(self.__completiontime)+"', total_time_taken= '"+str(self.__duration)+"'"
        db.exec_query(query)

    def make_task_background(self,pathname,funcname,*argv):
        Backtask.__createtask__(self)
        imp=importlib.import_module(pathname)
        instance=[k for k,v in locals().items() if v==imp][0]
        path=instance+'.'+funcname
        thread=threading.Thread(target=eval(path),args=(self,*argv))
        thread.daemon=True
        thread.start()
        print("\n\n######################## TASK "+str(self.__taskid)+"PUSHED TO background -SUCCESS")

    def __async_file_path_set__(self,pathname,funcname,*argv):
        imp=importlib.import_module(pathname)
        instance= [k for k,v in locals().items() if v==imp][0]
        path=instance+"."+funcname
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(eval(path)(self,*argv))

    def make_async_task_background(self,pathname,funcname,*argv):
        Backtask.__createtask__()
        thread= threading.Thread(target=Backtask.__async_file_path_set__,args=(self,pathname,funcname,*argv))
        thread.daemon=True
        thread.start()
        print("\n\n######################## TASK "+str(self.__taskid)+"PUSHED TO background -SUCCESS")

    def send_file_path(self, filepath):
        db=Backtask.__get_conn__()
        query="Update background_task set generated_file_path = '"+str(filepath)+"' where seq_num='"+str(self.__taskid)+"'"
        db.exec_query(query)

    def send_log_file_path(self,logpath):
        db=Backtask.__get_conn__()
        query="Update background_task set logfile_path = '"+str(logpath)+"' where seq_num='"+str(self.__taskid)+"'"
        db.exec_query(query)

    def set_task_message(self,message):
        mainmsg=""
        if(type(message) is dict):
            for k,v in message.items():
                mainmsg=mainmsg+str(k).strip()+" : "+str(v).strip()
        else:
            mainmsg=str(message)

        mainmsg=mainmsg.strip().replace("'","''")
        db=Backtask.__get_conn__()
        query="UPDATE background_task set response_message='"+str(mainmsg)+"' where seq_num='"+str(self.__taskid)+"'"
        db.exec_query(query)

    def send_file_name(self,filename):
        db = Backtask.__get_conn__()
        query = "Update background_task set generated_filename = '" + str(filename) + "' where seq_num='" + str(self.__taskid) + "'"
        db.exec_query(query)

    def get_taskid(self):
        return self.__taskid

    def get_task_by_id(self):
        cursor=Backtask.__get_conn__()
        query="select * from background_task where seq_num='"+str(self.__taskid)+"'"
        cursor.execute(query)
        res=cursor.fetchall()
        return res

    def __del__(self):
        Backtask.__taskcount-=1
        if(self.__status=="FINISHED" or self.__status=="FAILED"):
            Backtask.__end__()
        else:
            Backtask.updatestatus(self,"FAILED")
            Backtask.__end__()




