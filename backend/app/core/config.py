from pathlib import Path
import sys
import os
import re
from dotenv import load_dotenv
def get_base_dir():
    if getattr(sys,'frozen', False):
        return Path(sys.executable).parent.parent
    load_dotenv()
    
    return Path(__file__).resolve().parent.parent

class AppEnvironmentSetup:
    BASE_DIR = get_base_dir()
    ALLOWED_ORIGINS = [
    "http://localhost:5173",  # React/Next.js dev server
    "http://127.0.0.1:5173",
    "http://localhost:5176",
    "http://127.0.0.1:5176",
    "http://127.0.0.1:8080",
    "*"  # Local HTML file testing
    ]
    AI_MESSAGESEPARATOR = "<ai>"
    ACCESS_TOKEN_EXPIRY = os.getenv("ACCESS_TOKEN_EXPIRY")
    ALGORITHM = os.getenv("ALGORITHM")
    SECRET_KEY = os.getenv("SECRET_KEY")
    
    def __init__(self):
        self.attrs = [getattr(self,x) for x in dir(self) if not x.startswith('__')]
        self.index = 0
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.index == len(self.attrs):
            raise StopIteration
        _next = self.attrs[self.index]
        self.index+=1
        return _next
    
for env in AppEnvironmentSetup():
    print(f"Verifying environment settings for {env}")
    if isinstance(env,Path):
        print(f"Creating environment settings for {env}")
        os.makedirs(env,exist_ok=True)