from fastapi import FastAPI
import router
import asyncio

app = FastAPI()

@app.get('/')
async def main():
    return "Hello World!"

app.include_router(router.route)
asyncio.create_task(router.consume())
