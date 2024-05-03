use crate::settings::{PostgresConfig, Settings};
use crate::Result;
use color_eyre::eyre::eyre;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use rustls::ClientConfig as RustlsClientConfig;
use std::{fs::File, io::BufReader};
use tokio_postgres::{Config, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

#[derive(Clone, Debug)]
pub struct AppState {
    pub postgres_db: Pool,
    pub settings: Settings,
}

impl AppState {
    pub async fn new(postgres_db: Pool, settings: Settings) -> Result<Self> {
        Ok(Self {
            postgres_db,
            settings,
        })
    }
    pub async fn initialize(settings: Settings) -> Result<Self> {
        Ok(Self {
            postgres_db: postgres_connect(&settings.postgres).await?,
            settings,
        })
    }
}

pub async fn postgres_connect(postgres: &PostgresConfig) -> Result<Pool> {
    let mut pg_config: Config = Config::new();
    pg_config.password(postgres.password.expose());
    pg_config.user(&postgres.username);
    pg_config.host(&postgres.host);
    pg_config.port(postgres.port);
    pg_config.dbname(&postgres.database);

    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };

    let pool = if let Some(ca_cert) = postgres.ca_cert.expose() {
        let cert_file =
            File::open(ca_cert).map_err(|e| eyre!("Failed to open cert file: {}", e))?;
        let mut buf = BufReader::new(cert_file);
        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut buf) {
            root_store
                .add(cert.map_err(|e| eyre!("Failed to parse cert: {}", e))?)
                .map_err(|e| eyre!("Failed to add cert to store: {}", e))?;
        }

        let tls_config = RustlsClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let tls = MakeRustlsConnect::new(tls_config);
        let manager = Manager::from_config(pg_config, tls, manager_config);
        Pool::builder(manager).max_size(16).build().unwrap()
    } else {
        let manager = Manager::from_config(pg_config, NoTls, manager_config);
        Pool::builder(manager).max_size(16).build().unwrap()
    };

    Ok(pool)
}

pub async fn start(settings: Settings) -> Result<AppState> {
    let state = AppState::initialize(settings).await?;

    Ok(state)
}
