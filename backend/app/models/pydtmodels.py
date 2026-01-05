from pydantic import BaseModel, EmailStr


class Signup(BaseModel):
    username: str
    password: str
    name: str
    email: EmailStr

class login(BaseModel):
    username:str
    password:str