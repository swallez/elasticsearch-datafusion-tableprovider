// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Copy of datafusion-cli's main with an added Elasticsearch table provider.

// After updating the original code, add these lines after `ctx.register_udtf("parquet_metadata"...`
//
//  // register the `ELASTICSEARCH` table provider
//  dotenv::dotenv().ok();
//  elasticsearch_datafusion_tableprovider::ElasticsearchTableProviderFactory::register(&ctx);

//mod pg_table;

use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnvBuilder, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion_cli::catalog::DynamicObjectStoreCatalog;
use datafusion_cli::functions::ParquetMetadataFunc;
use datafusion_cli::{
    exec,
    pool_type::PoolType,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
    DATAFUSION_CLI_VERSION,
};

use clap::Parser;
// use mimalloc::MiMalloc;
//
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        value_parser(parse_valid_data_dir)
    )]
    data_path: Option<String>,

    #[clap(
        short = 'b',
        long,
        help = "The batch size of each query, or use DataFusion default",
        value_parser(parse_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(
        short = 'c',
        long,
        num_args = 0..,
        help = "Execute the given command string(s), then exit. Commands are expected to be non empty.",
        value_parser(parse_command)
    )]
    command: Vec<String>,

    #[clap(
        short = 'm',
        long,
        help = "The memory pool limitation (e.g. '10g'), default to None (no limit)",
        value_parser(extract_memory_pool_size)
    )]
    memory_limit: Option<usize>,

    #[clap(
        short,
        long,
        num_args = 0..,
        help = "Execute commands from file(s), then exit",
        value_parser(parse_valid_file)
    )]
    file: Vec<String>,

    #[clap(
        short = 'r',
        long,
        num_args = 0..,
        help = "Run the provided files on startup instead of ~/.datafusionrc",
        value_parser(parse_valid_file),
        conflicts_with = "file"
    )]
    rc: Option<Vec<String>>,

    #[clap(long, value_enum, default_value_t = PrintFormat::Automatic)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "Specify the memory pool type 'greedy' or 'fair'",
        default_value_t = PoolType::Greedy
    )]
    mem_pool_type: PoolType,

    #[clap(
        long,
        help = "The max number of rows to display for 'Table' format\n[possible values: numbers(0/10/...), inf(no limit)]",
        default_value = "40"
    )]
    maxrows: MaxRows,

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,
}

#[tokio::main]
/// Calls [`main_inner`], then handles printing errors and returning the correct exit code
pub async fn main() -> ExitCode {
    if let Err(e) = main_inner().await {
        println!("Error: {e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

/// Main CLI entrypoint
async fn main_inner() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("DataFusion CLI v{}", DATAFUSION_CLI_VERSION);
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p).unwrap();
    };

    let mut session_config = SessionConfig::from_env()?.with_information_schema(true);

    if let Some(batch_size) = args.batch_size {
        session_config = session_config.with_batch_size(batch_size);
    };

    let rt_config = RuntimeEnvBuilder::new();
    let rt_config =
        // set memory pool size
        if let Some(memory_limit) = args.memory_limit {
            // set memory pool type
            match args.mem_pool_type {
                PoolType::Fair => rt_config
                    .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit))),
                PoolType::Greedy => rt_config
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(memory_limit)))
            }
        } else {
            rt_config
        };

    let runtime_env = create_runtime_env(rt_config.clone())?;

    // enable dynamic file query
    let ctx =
        SessionContext::new_with_config_rt(session_config.clone(), Arc::new(runtime_env))
            .enable_url_table();
    ctx.refresh_catalogs().await?;
    // install dynamic catalog provider that can register required object stores
    ctx.register_catalog_list(Arc::new(DynamicObjectStoreCatalog::new(
        ctx.state().catalog_list().clone(),
        ctx.state_weak_ref(),
    )));
    // register `parquet_metadata` table function to get metadata from parquet files
    ctx.register_udtf("parquet_metadata", Arc::new(ParquetMetadataFunc {}));

    // register the `ELASTICSEARCH` table provider
    dotenv::dotenv().ok();
    elasticsearch_datafusion_tableprovider::ElasticsearchTableProviderFactory::register(&ctx);

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
        color: args.color,
    };

    let commands = args.command;
    let files = args.file;
    let rc = match args.rc {
        Some(file) => file,
        None => {
            let mut files = Vec::new();
            let home = dirs::home_dir();
            if let Some(p) = home {
                let home_rc = p.join(".datafusionrc");
                if home_rc.exists() {
                    files.push(home_rc.into_os_string().into_string().unwrap());
                }
            }
            files
        }
    };

    if commands.is_empty() && files.is_empty() {
        if !rc.is_empty() {
            exec::exec_from_files(&ctx, rc, &print_options).await?;
        }
        // TODO maybe we can have thiserror for cli but for now let's keep it simple
        return exec::exec_from_repl(&ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    if !files.is_empty() {
        exec::exec_from_files(&ctx, files, &print_options).await?;
    }

    if !commands.is_empty() {
        exec::exec_from_commands(&ctx, commands, &print_options).await?;
    }

    Ok(())
}

fn create_runtime_env(rn_config: RuntimeEnvBuilder) -> Result<RuntimeEnv> {
    rn_config.build()
}

fn parse_valid_file(dir: &str) -> std::result::Result<String, String> {
    if Path::new(dir).is_file() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn parse_valid_data_dir(dir: &str) -> std::result::Result<String, String> {
    if Path::new(dir).is_dir() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn parse_batch_size(size: &str) -> std::result::Result<usize, String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(size),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}

fn parse_command(command: &str) -> std::result::Result<String, String> {
    if !command.is_empty() {
        Ok(command.to_string())
    } else {
        Err("-c flag expects only non empty commands".to_string())
    }
}

#[derive(Debug, Clone, Copy)]
enum ByteUnit {
    Byte,
    KiB,
    MiB,
    GiB,
    TiB,
}

impl ByteUnit {
    fn multiplier(&self) -> u64 {
        match self {
            ByteUnit::Byte => 1,
            ByteUnit::KiB => 1 << 10,
            ByteUnit::MiB => 1 << 20,
            ByteUnit::GiB => 1 << 30,
            ByteUnit::TiB => 1 << 40,
        }
    }
}

fn extract_memory_pool_size(size: &str) -> std::result::Result<usize, String> {
    fn byte_suffixes() -> &'static HashMap<&'static str, ByteUnit> {
        static BYTE_SUFFIXES: OnceLock<HashMap<&'static str, ByteUnit>> = OnceLock::new();
        BYTE_SUFFIXES.get_or_init(|| {
            let mut m = HashMap::new();
            m.insert("b", ByteUnit::Byte);
            m.insert("k", ByteUnit::KiB);
            m.insert("kb", ByteUnit::KiB);
            m.insert("m", ByteUnit::MiB);
            m.insert("mb", ByteUnit::MiB);
            m.insert("g", ByteUnit::GiB);
            m.insert("gb", ByteUnit::GiB);
            m.insert("t", ByteUnit::TiB);
            m.insert("tb", ByteUnit::TiB);
            m
        })
    }

    fn suffix_re() -> &'static regex::Regex {
        static SUFFIX_REGEX: OnceLock<regex::Regex> = OnceLock::new();
        SUFFIX_REGEX.get_or_init(|| regex::Regex::new(r"^(-?[0-9]+)([a-z]+)?$").unwrap())
    }

    let lower = size.to_lowercase();
    if let Some(caps) = suffix_re().captures(&lower) {
        let num_str = caps.get(1).unwrap().as_str();
        let num = num_str.parse::<usize>().map_err(|_| {
            format!("Invalid numeric value in memory pool size '{}'", size)
        })?;

        let suffix = caps.get(2).map(|m| m.as_str()).unwrap_or("b");
        let unit = byte_suffixes()
            .get(suffix)
            .ok_or_else(|| format!("Invalid memory pool size '{}'", size))?;
        let memory_pool_size = usize::try_from(unit.multiplier())
            .ok()
            .and_then(|multiplier| num.checked_mul(multiplier))
            .ok_or_else(|| format!("Memory pool size '{}' is too large", size))?;

        Ok(memory_pool_size)
    } else {
        Err(format!("Invalid memory pool size '{}'", size))
    }
}
