use std::convert::TryFrom;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
enum InputFormatError {
    MissingAmount,
}

enum Error {
    InputFormat(InputFormatError),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(transparent)]
struct TransactionID {
    id: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(transparent)]
struct ClientID {
    id: u16,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
}

#[derive(Debug, Clone, Deserialize)]
struct NewTransaction {
    #[serde(alias = "type")]
    transaction_type: TransactionType,
    #[serde(alias = "client")]
    client_id: ClientID,
    #[serde(alias = "tx")]
    transaction_id: TransactionID,
    amount: Decimal,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum AmendmentType {
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Clone, Deserialize)]
struct TransactionAmendment {
    #[serde(alias = "type")]
    amendment_type: AmendmentType,
    #[serde(alias = "client")]
    client_id: ClientID,
    #[serde(alias = "tx")]
    transaction_id: TransactionID,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Record {
    Transaction(NewTransaction),
    Amendment(TransactionAmendment),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawRecordType {
    Transaction(TransactionType),
    Amendment(AmendmentType),
}

#[derive(Debug, Deserialize)]

struct RawRecord {
    #[serde(alias = "type")]
    record_type: RawRecordType,
    #[serde(alias = "client")]
    client_id: ClientID,
    #[serde(alias = "tx")]
    transaction_id: TransactionID,
    amount: Option<Decimal>,
}

impl std::convert::TryFrom<RawRecord> for Record {
    type Error = InputFormatError;
    fn try_from(raw_record: RawRecord) -> Result<Record, Self::Error> {
        match raw_record.record_type {
            RawRecordType::Amendment(amendment_type) => {
                if raw_record.amount.is_some() {
                    eprintln!(
                        "Warning: amount info on transation {:?} will be ignored",
                        &raw_record
                    );
                }
                Ok(Record::Amendment(TransactionAmendment {
                    amendment_type,
                    client_id: raw_record.client_id,
                    transaction_id: raw_record.transaction_id,
                }))
            }
            RawRecordType::Transaction(transaction_type) => match raw_record.amount {
                Some(amount) => Ok(Record::Transaction(NewTransaction {
                    transaction_type,
                    amount,
                    client_id: raw_record.client_id,
                    transaction_id: raw_record.transaction_id,
                })),
                None => Err(InputFormatError::MissingAmount),
            },
        }
    }
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
    type Item = Record;

    fn next(self: &mut Self) -> Option<Record> {
        // TODO: this definitely can be better structured,
        // probs after we can make the error from any internal error
        if let Some(res) = self.csv_reader.deserialize::<RawRecord>().next() {
            match res {
                Ok(raw_record) => match Record::try_from(raw_record) {
                    Ok(record) => Some(record),
                    Err(err) => {
                        eprintln!("CSV parsing error: {:?}", &err);
                        None
                    }
                },
                Err(err) => {
                    eprintln!("CSV parsing error: {:?}", &err);
                    None
                }
            }
        } else {
            None
        }
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
