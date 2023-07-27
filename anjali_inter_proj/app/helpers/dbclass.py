from fastapi import HTTPException
import sqlalchemy
import pandas as pd

class DB():
    def __init__(self,details):
        self.dbtype=details['dbtype']
        self.driver=details['driver']
        self.username=details['username']
        self.host=details['host']
        self.password=details['password']
        self.port=details['port']
        self.sid=details['sid']
        self.connstatus='NON_CONNECTED'
        self.create_db_engine()

    def create_db_engine(self):
        DIALECT = self.dbtype
        DRIVER = self.driver
        USERNAME = self.username
        PASSWORD = self.password
        HOST = self.host
        PORT = self.port
        SERVICE = self.sid

        if(DIALECT=='oracle'):
            conn = DIALECT + '+' + DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(
                PORT) + '/?service_name=' + SERVICE
        elif(DIALECT=='postgresql'):
            conn = DIALECT + '+' + DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(
                PORT) + '/' + SERVICE + '?'
        else:
            raise HTTPException(status_code=400, detail={"error":"Unsupported DB type"})
        
        try:
            self.engine = sqlalchemy.create_engine(conn)
            self.connstatus='CONNECTED'
        except Exception as e:
            self.connstatus='ERROR'
            raise HTTPException(status_code=400, detail={"error":f"Could not connect to database as this error was encountered{e}"})
        
    def get_engine(self):
        return self.engine
    
    def exec_query(self,query):
        try:
            self.engine.execute(query)
            return {"msg":"Successfully Inserted"}
        except Exception as e:
            raise HTTPException(status_code=400, detail={"error":f"Could not execute query: {query} for {str(self.engine)} as: {str(e)}"})
        
    def exec_with_exception(self, query):
        try:
            self.engine.execute(query)
            return{"msg":"Successfully Executed"}
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error":f"{e}"})
        
    def get_all_data_as_df(self,query,keycase):
        allowed_keys=['sentence','lower','upper','original']
        if keycase not in allowed_keys:
            raise HTTPException(status_code=400, detail={"error":f"Keycase {keycase} is not supported"})
        
        try:
            data = pd.read_sql(query, self.engine)

            if data.empty:
                return data
            
            if keycase == 'upper':
                data.columns=data.columns.str.upper()
            
            if keycase == 'lower':
                data.columns=data.columns.str.lower()

            if keycase == 'sentence':
                data.columns=data.columns.str.title()

            return data
        except Exception as e:
            raise HTTPException(status_code=400, detail={"error":f"Could not execute query: {query} for {str(self.engine)} as: {str(e)}"})

    def get_data(self,query):
        try:
            resobj=self.engine.execute(query)
            data=resobj.fetchall()
            return data
        except Exception as ex:
            print("get data failed error= ",ex)
            raise HTTPException(status_code=400, detail={"error":f"Could not execute query: {query} for {str(self.engine)} as: {str(ex)}"})

    def get_one_data(self,query):
        print(query)
        try:
            resobj=self.engine.execute(query)
            data=resobj.fetchone()
            return data
        except Exception as ex:
            raise HTTPException(status_code=400, detail={"error":f"Could not execute query: {query} for {str(self.engine)} as: {str(ex)}"})

    def exec_query_bind(self,query,dictionary):
        try:
            self.engine.execute(text(query),dictionary)
        except Exception as ex:
            print(f"Could not execute query: {query} for {str(self.engine)} as: {str(ex)} ")
            raise HTTPException(status_code=400, detail={"error":f"Could not execute query: {query} for {str(self.engine)} as: {str(ex)}"})

    def info(self):
        output=dict(
            db_type=self.dbtype,
            driver=self.driver,
            hostname=self.host,
            sid=self.sid,
            port=self.port,
            connection=self.connstatus
        )
        return output

    def exec_get_resultset(self,query):
        result= {'rows':[]}
        try:
            rows=DB.get_data(self, query)
            for r in rows:
                values=[]
                for t in r:
                    values.append(t)
                result['rows'].append(values)
            return result
        except Exception as ex:
            print(f"Could not execute query: {query} for {str(self.engine)} as: {str(ex)}")
            raise HTTPException(status_code=400, detail={"error":f"Could not execute query: {query} for {str(self.engine)} as: {str(ex)}"})

    def __del__(self):
        try:
            self.engine.dispose()
        except Exception as ex:
            print(f"!! WARNING !! Connection to {str(self.engine)} has not been closed  as: {str(ex)}")
