"""
Created on : 23 June 2021
@Autor : Anjali Tripathi
"""

from fastapi import FastAPI
from app.endpoints import backgrounddownload
 
app = FastAPI(title = "My Research API")

@app.get("/")
def root():
    return {"INFO":"Welcome to Anjali's API dashboard"}

app.include_router(backgrounddownload.router, prefix="/extras")