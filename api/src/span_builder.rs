use actix_web::{
    Error,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
};
use tracing::{Span, info};
use tracing_actix_web::{DefaultRootSpanBuilder, RootSpanBuilder};

/// The `RootSpanBuilder` implementation for the API service.
///
/// It extracts the project ref from the `tenant_id` header and sets it as a field in the root span.
/// It also emits info logs for request start and completion.
#[derive(Debug)]
pub struct ApiRootSpanBuilder;

impl RootSpanBuilder for ApiRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> Span {
        let project = request.headers().get("tenant_id");
        let span = match project {
            Some(project) => {
                // We convert lossily to a string to be able to read at least part of the project
                // ref in case of invalid UTF-8. This is useful for debugging.
                //
                // However, this is an edge case, as the project ref is generated by the system and
                // should be valid UTF-8.
                let project = String::from_utf8_lossy(project.as_bytes());
                let project = project.as_ref();
                tracing_actix_web::root_span!(request, project = project)
            }
            None => tracing_actix_web::root_span!(request, project = tracing::field::Empty),
        };

        // We enter the span temporarily to log the request received. The span will be entered
        // automatically before the request is handled.
        {
            let _enter = span.enter();
            info!(
                method = %request.method(),
                uri = %request.uri(),
                path = %request.path(),
                query_string = %request.query_string(),
                version = ?request.version(),
                "HTTP request received"
            );
        }

        span
    }

    fn on_request_end<B: MessageBody>(span: Span, outcome: &Result<ServiceResponse<B>, Error>) {
        // We call the default builder to add more details to the current span.
        DefaultRootSpanBuilder::on_request_end(span, outcome);

        // In case we have a positive response, we want to log the success. In case of error, it will
        // be logged automatically since we have the `emit_event_on_error` feature enabled.
        if let Ok(response) = outcome {
            if response.response().error().is_none() {
                info!("HTTP request completed successfully");
            }
        }
    }
}
