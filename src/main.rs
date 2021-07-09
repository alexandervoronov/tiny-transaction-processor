use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct TransactionID {
    id: u32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct ClientID {
    id: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RecordType {
    Deposit,
    Withdrawal,
}
#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(alias = "type")]
    record_type: RecordType,
    #[serde(alias = "client")]
    client_id: ClientID,
    #[serde(alias = "tx")]
    transaction_id: u32,
    amount: Option<Decimal>,
}

struct CsvReader {
    csv_reader: csv::Reader<std::fs::File>,
}

impl CsvReader {
    // TODO: should accept path and skip the File::open
    fn new(filepath: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            csv_reader: csv::ReaderBuilder::new()
                .trim(csv::Trim::All)
                .from_reader(std::fs::File::open(filepath)?),
        })
    }
}

impl std::iter::Iterator for CsvReader {
    type Item = Transaction;

    fn next(self: &mut Self) -> Option<Transaction> {
        self.csv_reader
            .deserialize()
            .next()
            .transpose()
            .map_err(|err| eprintln!("CSV parsing error: {}", &err))
            .ok()
            .flatten()
    }
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  tiny-transaction-processor <path-to-transaction-file>");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args();
    if args.len() < 2 {
        eprintln!("Error: missing path to the transaction file");
        eprintln!("");
        print_usage();
    } else if args.len() > 2 {
        eprintln!("Error: only one command line argument is expected");
        eprintln!("");
        print_usage();
    } else {
        let filename = args.skip(1).next().unwrap();
        eprintln!("Got file {}", &filename);

        let csv_records = CsvReader::new(&filename)?;
        for record in csv_records.into_iter() {
            eprintln!("Banana: {:#?}", &record);
        }
    }

    Ok(())
}
