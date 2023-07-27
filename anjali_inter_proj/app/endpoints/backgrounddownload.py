from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
import os


router= APIRouter(
    prefix="/filedownload",
    tags=["Download"],
    responses={
        404:{"Description":"Not found"},
        500:{"Error":"Internal Server Error"},
        400:{"Description":"Records not found"}
    }
)
allowed_files=['xlsx', 'xls', 'xlsv', 'xlsm', 'txt', 'json', 'dox', 'docs', 'csv', 'log']

@router.get("/download")
async def filedownload(file_location:str,filename:str):
    if '.' not in file_location:
        raise HTTPException(status_code=400,detail={"errpr":"Could not find any extension"})

    extn= file_location.rsplit('.')[-1].lower()
    if extn not in allowed_files:
        raise HTTPException(status_code=400,detail={"error":f"Cannot downloaad .{str(extn)} type files"})
    if(filename=="" or filename=="(null)" or filename==None or filename=="NIL"):
        filename=f"Download.{str(extn)}"

    fileext=file_location.rsplit('.')[-1].lower()
    if(fileext!=extn):
        raise HTTPException(status_code=400,detail={"error":"Extension of file name and path name do not match"})

    if os.path.isfile(file_location):
        x=FileResponse(file_location,filename=filename)
        return x
    else:
        raise HTTPException(status_code=404,detail={"erro":"Does not exist"})


