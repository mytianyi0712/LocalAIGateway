import secrets

from fastapi import Header, HTTPException, Request

from app.core.access import resolve_access_policy


async def get_session(request: Request):
    async with request.app.state.db.sessions() as session:
        yield session


async def require_admin(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    policy = await resolve_access_policy(request.app)
    if policy["trust_local_network"]:
        return
    expected = f"Bearer {policy['admin_key']}"
    if not authorization or not secrets.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Invalid admin token")
