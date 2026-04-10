use std::sync::Arc;

use tokio::sync::RwLock;

mod banner;
mod config;
mod signals;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,strata=debug".parse().unwrap()),
        )
        .init();

    // Install Prometheus metrics recorder
    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    banner::print();

    let server_config = config::load()?;

    let engine = Arc::new(strata_core::StrataEngine::new(server_config.core).await?);

    // Start Raft cluster if enabled
    let coordinator = Arc::new(RwLock::new(
        strata_cluster::ClusterCoordinator::new(server_config.cluster.clone()),
    ));

    if server_config.cluster.enabled {
        let mut coord = coordinator.write().await;
        coord.start_raft(engine.clone()).await?;
        drop(coord);
    }

    let cluster_handle = if server_config.cluster.enabled {
        Some(coordinator.clone())
    } else {
        None
    };

    let gateway = strata_gateway::GatewayServer::start(
        engine.clone(),
        server_config.gateway,
        Some(prometheus_handle),
        cluster_handle,
    )
    .await?;

    signals::wait_for_shutdown().await;

    coordinator.write().await.shutdown().await?;
    gateway.shutdown().await?;
    Arc::try_unwrap(engine)
        .map_err(|_| anyhow::anyhow!("engine still has active references"))?
        .shutdown()
        .await?;

    tracing::info!("Strata shutdown complete");
    Ok(())
}
