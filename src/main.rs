use log::{error, info};
use tiny_transaction_processor::*;

fn print_usage() {
    info!("Usage:");
    info!("  tiny-transaction-processor <path-to-transaction-file>");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp(None)
        .format_module_path(false)
        .parse_default_env()
        .init();

    let mut args = std::env::args();
    match args.len().cmp(&2) {
        std::cmp::Ordering::Equal => {
            let filename = args.nth(1).unwrap();
            info!("Input CSV file: {}", &filename);

            let csv_transactions = CsvReader::from_path(&std::path::Path::new(&filename))?;
            let mut transaction_processor = TransactionProcessor::default();
            for transaction in csv_transactions.into_iter() {
                if let Err(err) = transaction_processor.process(&transaction) {
                    error!("[ {} ] failed with error {:?}", &transaction, &err);
                }
            }

            let stdout = std::io::stdout();
            let stdout_lock = stdout.lock();
            let mut csv_account_writer = csv::Writer::from_writer(stdout_lock);
            for (client_id, account) in transaction_processor.accounts.iter() {
                csv_account_writer.serialize(AccountWithClientID { client_id, account })?;
            }

            Ok(())
        }
        std::cmp::Ordering::Greater => {
            error!("Only one command line argument is expected");
            eprintln!();
            print_usage();

            Err(std::io::Error::from(std::io::ErrorKind::InvalidInput).into())
        }
        std::cmp::Ordering::Less => {
            error!(
                "Missing argument! Please provide a path to the CSV file containing transactions"
            );
            eprintln!();
            print_usage();

            Err(std::io::Error::from(std::io::ErrorKind::InvalidInput).into())
        }
    }
}
