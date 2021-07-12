use tiny_transaction_processor::*;

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  tiny-transaction-processor <path-to-transaction-file>");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args();
    match args.len().cmp(&2) {
        std::cmp::Ordering::Equal => {
            let filename = args.skip(1).next().unwrap();
            eprintln!("Got file {}", &filename); // TODO: replace with log trace

            let csv_transactions = CsvReader::from_path(&std::path::Path::new(&filename))?;
            let mut transaction_processor = TransactionProcessor::default();
            for transaction in csv_transactions.into_iter() {
                if let Err(err) = transaction_processor.process(&transaction) {
                    eprintln!(
                        "Transaction [{:?}] processing error: {:?}",
                        &transaction, &err
                    );
                }
            }

            let stdout = std::io::stdout();
            let stdout_lock = stdout.lock();
            let mut csv_account_writer = csv::Writer::from_writer(stdout_lock);
            for (client_id, account) in transaction_processor.accounts.iter() {
                csv_account_writer.serialize(AccountWithClientID { client_id, account })?;
            }
        }
        std::cmp::Ordering::Greater => {
            // TODO: return error here? 
            eprintln!("Error: only one command line argument is expected");
            eprintln!("");
            print_usage();
        }
        std::cmp::Ordering::Less => {
            eprintln!("Error: missing path to the transaction file");
            eprintln!("");
            print_usage();
        }
    }

    Ok(())
}
