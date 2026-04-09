pub async fn run(_url: &str, from: &str) -> anyhow::Result<()> {
    println!("Restore source: {from}");
    println!("Note: restore requires direct access to the Strata data directory");
    println!(
        "      Use 'docker compose exec strata strata-server restore' for production restores"
    );

    Ok(())
}
