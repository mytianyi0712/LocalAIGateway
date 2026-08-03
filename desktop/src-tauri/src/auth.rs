use axum::{extract::FromRequestParts, http::request::Parts};

use crate::{api_error::ApiError, server::AppState, settings};

pub struct AdminAuth;

impl FromRequestParts<AppState> for AdminAuth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        match settings::authorize_admin(state, &parts.headers).await {
            Ok(true) => Ok(Self),
            Ok(false) => Err(ApiError::unauthorized()),
            Err(error) => Err(ApiError::internal(error)),
        }
    }
}
