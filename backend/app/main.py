from fastapi import FastAPI

app = FastAPI(
    title="My FastAPI NotePad Backend",
    debug=True,
    description="API Documentation for notePad application",
    version="1.0.0",
    openapi_tags=[
        {"name": "Auth", "description": "Authentication related endpoints"},

    ]
    # lifespan=lifespan
)
