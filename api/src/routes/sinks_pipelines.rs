use actix_web::{
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    db::{
        self,
        pipelines::PipelineConfig,
        sinks::{sink_exists, SinkConfig},
        sinks_pipelines::SinkPipelineDbError,
        sources::source_exists,
    },
    encryption::EncryptionKey,
    routes::extract_tenant_id,
};

use super::{sinks::SinkError, ErrorMessage, TenantIdError};

#[derive(Deserialize, ToSchema)]
pub struct PostSinkPipelineRequest {
    #[schema(example = "Sink Name", required = true)]
    pub sink_name: String,

    #[schema(required = true)]
    pub sink_config: SinkConfig,

    #[schema(required = true)]
    pub source_id: i64,

    #[schema(required = true)]
    pub publication_name: String,

    #[schema(required = true)]
    pub pipeline_config: PipelineConfig,
}

#[derive(Debug, Error)]
enum SinkPipelineError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("no default image found")]
    NoDefaultImageFound,

    #[error("tenant id error: {0}")]
    TenantId(#[from] TenantIdError),

    #[error("source with id {0} not found")]
    SourceNotFound(i64),

    #[error("sink with id {0} not found")]
    SinkNotFound(i64),

    #[error("pipeline with id {0} not found")]
    PipelineNotFound(i64),

    #[error("sinks error: {0}")]
    Sink(#[from] SinkError),

    #[error("sinks and pipelines db error: {0}")]
    SinkPipelineDb(#[from] SinkPipelineDbError),
}

impl SinkPipelineError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            SinkPipelineError::DatabaseError(_) | SinkPipelineError::SinkPipelineDb(_) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for SinkPipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            SinkPipelineError::Sink(e) => e.status_code(),
            SinkPipelineError::DatabaseError(_)
            | SinkPipelineError::NoDefaultImageFound
            | SinkPipelineError::SinkPipelineDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
            SinkPipelineError::TenantId(_)
            | SinkPipelineError::SourceNotFound(_)
            | SinkPipelineError::SinkNotFound(_)
            | SinkPipelineError::PipelineNotFound(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let error_message = ErrorMessage {
            error: self.to_message(),
        };
        let body =
            serde_json::to_string(&error_message).expect("failed to serialize error message");
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(body)
    }
}

#[derive(Serialize, ToSchema)]
pub struct PostSinkPipelineResponse {
    sink_id: i64,
    pipeline_id: i64,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostSinkPipelineRequest,
    responses(
        (status = 200, description = "Create a new sink and a pipeline", body = PostSinkPipelineResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/sinks-pipelines")]
pub async fn create_sinks_and_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
    sink_and_pipeline: Json<PostSinkPipelineRequest>,
    encryption_key: Data<EncryptionKey>,
) -> Result<impl Responder, SinkPipelineError> {
    let sink_and_pipeline = sink_and_pipeline.0;
    let PostSinkPipelineRequest {
        sink_name,
        sink_config,
        source_id,
        publication_name,
        pipeline_config,
    } = sink_and_pipeline;
    let tenant_id = extract_tenant_id(&req)?;

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(SinkPipelineError::SourceNotFound(source_id));
    }

    let image = db::images::read_default_image(&pool)
        .await?
        .ok_or(SinkPipelineError::NoDefaultImageFound)?;
    let (sink_id, pipeline_id) = db::sinks_pipelines::create_sink_and_pipeline(
        &pool,
        tenant_id,
        source_id,
        &sink_name,
        sink_config,
        image.id,
        &publication_name,
        pipeline_config,
        &encryption_key,
    )
    .await?;
    let response = PostSinkPipelineResponse {
        sink_id,
        pipeline_id,
    };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostSinkPipelineRequest,
    responses(
        (status = 200, description = "Update a sink and a pipeline", body = PostSinkPipelineResponse),
        (status = 404, description = "Pipeline or sink not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/sinks-pipelines/{sink_id}/{pipeline_id}")]
pub async fn update_sinks_and_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
    sink_and_pipeline_ids: Path<(i64, i64)>,
    sink_and_pipeline: Json<PostSinkPipelineRequest>,
    encryption_key: Data<EncryptionKey>,
) -> Result<impl Responder, SinkPipelineError> {
    let sink_and_pipeline = sink_and_pipeline.0;
    let PostSinkPipelineRequest {
        sink_name,
        sink_config,
        source_id,
        publication_name,
        pipeline_config,
    } = sink_and_pipeline;
    let tenant_id = extract_tenant_id(&req)?;
    let (sink_id, pipeline_id) = sink_and_pipeline_ids.into_inner();

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(SinkPipelineError::SourceNotFound(source_id));
    }

    if !sink_exists(&pool, tenant_id, sink_id).await? {
        return Err(SinkPipelineError::SinkNotFound(sink_id));
    }

    db::sinks_pipelines::update_sink_and_pipeline(
        &pool,
        tenant_id,
        sink_id,
        pipeline_id,
        source_id,
        &sink_name,
        sink_config,
        &publication_name,
        pipeline_config,
        &encryption_key,
    )
    .await
    .map_err(|e| match e {
        SinkPipelineDbError::SinkNotFound(sink_id) => SinkPipelineError::SinkNotFound(sink_id),
        SinkPipelineDbError::PipelineNotFound(pipeline_id) => {
            SinkPipelineError::PipelineNotFound(pipeline_id)
        }
        e => e.into(),
    })?;

    Ok(HttpResponse::Ok().finish())
}
