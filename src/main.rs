use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand, ValueEnum};
use quorid::{Allocator, AllocatorConfig, Backend, FileBackend, Format, Id, MemoryBackend};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "quorid",
    version,
    about = "Globally unique 128-bit IDs from a replicated backend"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the ID server.
    Serve(ServeArgs),
    /// Encode a decimal ID.
    Encode {
        id: Id,
        #[arg(short, long, value_enum, default_value_t = Format::Mania)]
        format: Format,
        /// Fixed-width output (lexicographically sortable for ASCII formats).
        #[arg(long)]
        padded: bool,
    },
    /// Decode an ID and print it in every format.
    Decode {
        value: String,
        #[arg(short, long, value_enum, default_value_t = Format::Mania)]
        format: Format,
    },
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, env = "QUORID_LISTEN", default_value = "127.0.0.1:7070")]
    listen: SocketAddr,
    #[arg(long, env = "QUORID_BACKEND", value_enum, default_value_t = BackendKind::File)]
    backend: BackendKind,
    /// Directory for the `file` backend.
    #[arg(long, env = "QUORID_DATA_DIR", default_value = "quorid-data")]
    data_dir: PathBuf,
    /// IDs leased from the backend per round trip.
    #[arg(long, env = "QUORID_BLOCK_SIZE", default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..))]
    block_size: u64,
    /// Default encoding of returned IDs.
    #[arg(long, env = "QUORID_FORMAT", value_enum, default_value_t = Format::Mania)]
    format: Format,
}

#[derive(Clone, Copy, ValueEnum)]
enum BackendKind {
    /// Volatile, for tests and demos.
    Memory,
    /// Durable single node.
    File,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Serve(args) => match args.backend {
            BackendKind::Memory => serve(MemoryBackend::new(), args).await,
            BackendKind::File => serve(FileBackend::open(&args.data_dir)?, args).await,
        },
        Command::Encode { id, format, padded } => {
            println!("{}", format.encode(id, padded));
            Ok(())
        }
        Command::Decode { value, format } => {
            let id = format.decode(&value)?;
            for f in Format::ALL {
                println!("{:>8}  {}", f.to_string(), f.encode(id, false));
            }
            Ok(())
        }
    }
}

async fn serve<B: Backend>(backend: B, args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let status = backend.status();
    let allocator = Arc::new(Allocator::new(
        Arc::new(backend),
        AllocatorConfig::with_block_size(args.block_size),
    ));
    let app = quorid::server::router(allocator, args.format);

    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    // The bound address differs from `--listen` only for port 0; querying a
    // bound socket does not fail in practice.
    let addr = listener.local_addr().unwrap_or(args.listen);
    tracing::info!(
        %addr,
        backend = status.kind,
        durable = status.durable,
        block_size = args.block_size,
        "quorid listening"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await
        .map_err(Into::into)
}
