from fastapi import APIRouter, Depends, HTTPException, Body
from app.core.commonExceptions import exceptionHandler
from app.models.pydtmodels import Signup, login
from app.services.authService import validateLogin, signupUser

router = APIRouter()


@router.post("/signup", tags=['Auth'])
@exceptionHandler
async def user_registration(body:Signup):
    """
    User Registration Endpoint.

    Args:
        body (The requestData): validation using Pydantic model containing user details for registration.

    Returns:
        dict: Message indicating successful user registration.

    Raises:
        HTTPException: If the user already exists or if signup fails.

    Sample Body:
    ```
    {
        "name":"foo",
        "email":"foo@example.com",
        "password":"flalala"
    }
    ```
    """
    result = await signupUser(details=body)
    if result:
        return {"message": "User has signed up successfully", "errors": None}
    else:
        raise HTTPException(status_code=405,detail={'errors':"Signup failed", "message":"Cannot complete signup at this time please try again later."})




@router.post("/login", tags=['Auth'])
@exceptionHandler
async def user_login(creds:login):
    """
    User login Endpoint.

    Args:
        creds (The requestData): validation using Pydantic model containing user details for registration.

    Returns:
        dict: Message indicating successful user registration.

    Raises:
        HTTPException: If the user already exists or if signup fails.
    """
    result = await validateLogin(creds=creds)
    if result:
        return {"message": "User has signed up successfully", "errors": None}
    else:
        raise HTTPException(status_code=405,detail={'errors':"Signup failed", "message":"Cannot complete signup at this time please try again later."})

