use crate::client::StrataClient;

pub async fn run(url: &str, target: &str) -> anyhow::Result<()> {
    let client = StrataClient::new(url);

    // Get event count as a simple backup verification
    let result = client
        .query("SELECT count(*)::VARCHAR as cnt FROM episodic")
        .await?;
    let count = result["rows"]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row["cnt"].as_str())
        .unwrap_or("0");

    println!("Backup target: {target}");
    println!("Events to backup: {count}");
    println!("Note: full backup requires S3 storage configuration");
    println!("      Use 'docker compose exec strata strata-server backup' for production backups");

    Ok(())
}
