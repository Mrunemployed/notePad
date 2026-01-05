from passlib.context import CryptContext
from app.core.common import APIS
from datetime import datetime, timedelta, timezone
from app.core.config import AppEnvironmentSetup
import jwt
from app.models.models import schemas
from fastapi import HTTPException

passContext = CryptContext(schemes=["bcrypt"])
EXPIRY = AppEnvironmentSetup.ACCESS_TOKEN_EXPIRY
HASHING_ALGORITHM = AppEnvironmentSetup.ALGORITHM
SECRET = AppEnvironmentSetup.SECRET_KEY

def hash_password(password:str) -> str:
    return passContext.hash(password)

def verify_password(unhashed_password: str, hashed_password: str) -> bool:
    return passContext.verify(unhashed_password,hashed_password)

async def _create_jwt_token(data:dict,expiry:timedelta=None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=expiry or EXPIRY)
    to_encode.update({"exp":expire})
    return jwt.encode(to_encode,SECRET,HASHING_ALGORITHM)


async def decode_access_token(token:str) -> dict|None:
    try:
        payload = jwt.decode(token,SECRET,HASHING_ALGORITHM)
        print(payload)
        if not isinstance(payload,dict):
            print('Invalid or Fake JWT')
            return 
        elif isinstance(payload,dict) and not all([x for y,x in payload.items() ]) and payload.get("user_type")!="admin":
            print("<DENIED> JWT Spoofing detected")
            raise HTTPException(status_code=401, detail={"errors":"Invalid Token", "message":"Your session token is invalid, warning spoofing detected"})
        
        db_record = await APIS.db_get(model=schemas.USERS, user_id=payload.get('user_id'))

        if not db_record:
            raise HTTPException(status_code=401, detail={"errors":"Invalid Token", "message":"Your session token is invalid, you do not have an active account with us"})
        
        db_verification_record = db_record[0]
        if not db_verification_record.get('plan_expiry') == payload.get('plan_expiry'):
            raise HTTPException(status_code=401, detail={"errors":"Invalid Token", "message":"Your session token is invalid, warning spoofing detected"})
        payload.update({'plan_expiry': db_verification_record.get('plan_expiry')})
        return payload
    except jwt.ExpiredSignatureError:
        print("JWT Token expired")
        raise HTTPException(status_code=401, detail={"errors":"Token Expired", "message":"Your session has expired please login again."})

    except jwt.InvalidTokenError:
        print("INVALID JWT Token")
        raise HTTPException(status_code=401, detail={"errors":"Invalid Token", "message":"Your session token is invalid, warning spoofing detected"})
