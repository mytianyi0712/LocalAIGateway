from app.services.settings import get_access_policy


async def resolve_access_policy(app):
    async with app.state.db.sessions() as session:
        return await get_access_policy(
            session,
            app.state.secrets,
            app.state.settings.admin_token,
            app.state.settings.gateway_key,
        )
