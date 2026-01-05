from app.models.models import schemas
import asyncio
from sqlalchemy import event
from app.core.startup import Rbac
import asyncpg
from app.core.config import AppEnvironmentSetup as appset

def refresh_rbac_cache(*args,**kwargs):
    loop = asyncio.get_event_loop()
    loop.create_task(Rbac.refresh())


async def listen_for_rbac_changes():
    conn = await asyncpg.connect(appset.DB_EVENT_LISTENER_CONN_STRING)
    # Listen on a channel, e.g., "rbac_updates"
    await conn.add_listener("rbac_updates", refresh_rbac_cache)
    try:
        # Keep the connection alive to continue receiving notifications
        while True:
            await asyncio.sleep(10)
    finally:
        await conn.close()

event.listen(schemas.RBAC, "after_insert", refresh_rbac_cache)
event.listen(schemas.RBAC, "after_update", refresh_rbac_cache)
event.listen(schemas.RBAC, "after_delete", refresh_rbac_cache)