from app.models.pydtmodels import Signup, login
from app.models.models import schemas
from app.core.common import APIS
from fastapi import HTTPException

async def validateLogin(creds:login):
    ...

async def signupUser(details:Signup):
    details_dict = details.model_dump()
    find = APIS.db_get(model=schemas.USERS, username=details.username, email=details.email)
    if find:
        raise HTTPException("User already exists!")
    result = await APIS.db_post(action="insert",model=schemas.USERS, **details_dict)
    return result