use std::convert::TryFrom;

use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
enum InputFormatError {
    MissingAmount,
}

#[derive(Debug)]
enum ProcessingError {
    TransactionOnLockedAccount,
    NotEnoughMoneyForWithdrawal,
    TryingToDisputeUnknownTransaction,
    WrongClientInDispute,
    TransactionIsAlreadyInDispute,
    ResolvedTransactionWasNotInDispute,
    ChargedBackWasNotInDispute,
}

#[derive(Debug)]
enum Error {
    InputFormat(InputFormatError),
    Processing(ProcessingError),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
struct TransactionID {
    id: u32,
}

impl TransactionID {
    #[cfg(test)]
    fn new(id: u32) -> TransactionID {
        TransactionID { id }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
struct ClientID {
    id: u16,
}

impl ClientID {
    #[cfg(test)]
    fn new(id: u16) -> ClientID {
        ClientID { id }
    }
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

impl Record {
    #[cfg(test)]
    fn transaction_id(&self) -> TransactionID {
        match self {
            Record::Transaction(transaction) => transaction.transaction_id,
            Record::Amendment(amendment) => amendment.transaction_id,
        }
    }
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
    fn new(filepath: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_path(filepath)
            .map(|reader| Self { csv_reader: reader })
            .map_err(|err| err.into())
    }
}

impl std::iter::Iterator for CsvReader {
    type Item = Record;

    fn next(self: &mut Self) -> Option<Record> {
        // TODO: this definitely can be better structured,
        // probs after we can make the error from any internal error
        if let Some(res) = self.csv_reader.deserialize::<RawRecord>().next() {
            match res {
                Ok(raw_record) => Record::try_from(raw_record)
                    .map_err(|err| {
                        eprintln!("CSV parsing error: {:?}", &err);
                        err
                    })
                    .ok(),
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Account {
    available: Decimal,
    held: Decimal,
    locked: bool,
}

impl Account {
    fn total(&self) -> Decimal {
        self.available + self.held
    }
}

#[derive(Debug, Clone)]
struct AccountRecord<'a> {
    client_id: &'a ClientID,
    account: &'a Account,
}

impl<'a> Serialize for AccountRecord<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("AccountRecord", 5)?;
        state.serialize_field("client", &self.client_id)?;
        state.serialize_field("available", &self.account.available)?;
        state.serialize_field("held", &self.account.held)?;
        state.serialize_field("total", &self.account.total())?;
        state.serialize_field("locked", &self.account.locked)?;
        state.end()
    }
}

#[derive(Default)]
struct TransactionProcessor {
    // TODO: disallow double chargeback of the same transaction
    transactions: std::collections::HashMap<TransactionID, NewTransaction>,
    accounts: std::collections::HashMap<ClientID, Account>,
    in_dispute: std::collections::HashSet<TransactionID>,
}

impl TransactionProcessor {
    fn process(&mut self, record: &Record) -> Result<(), Error> {
        // TODO: test for challenging the withdrawal that wasn't possible
        match record {
            Record::Transaction(transaction) => {
                let mut client_account = self
                    .accounts
                    .get(&transaction.client_id)
                    .cloned()
                    .unwrap_or_default();

                if client_account.locked {
                    return Err(Error::Processing(
                        ProcessingError::TransactionOnLockedAccount,
                    ));
                }
                match transaction.transaction_type {
                    TransactionType::Deposit => client_account.available += transaction.amount,
                    TransactionType::Withdrawal => {
                        if client_account.available >= transaction.amount {
                            client_account.available -= transaction.amount;
                        } else {
                            return Err(Error::Processing(
                                ProcessingError::NotEnoughMoneyForWithdrawal,
                            ));
                        }
                    }
                }
                self.accounts.insert(transaction.client_id, client_account);
                self.transactions
                    .insert(transaction.transaction_id, transaction.clone());
                Ok(())
            }
            Record::Amendment(amendment) => {
                let transaction = match self.transactions.get(&amendment.transaction_id) {
                    Some(transaction) => transaction,
                    None => {
                        return Err(Error::Processing(
                            ProcessingError::TryingToDisputeUnknownTransaction,
                        ))
                    }
                };
                if transaction.client_id != amendment.client_id {
                    return Err(Error::Processing(ProcessingError::WrongClientInDispute));
                }

                let mut client_account = self
                    .accounts
                    .get(&amendment.client_id)
                    .cloned()
                    .expect("Client account must be present for recognised transactions");

                match amendment.amendment_type {
                    AmendmentType::Dispute => {
                        if !self.in_dispute.insert(amendment.transaction_id) {
                            return Err(Error::Processing(
                                ProcessingError::TransactionIsAlreadyInDispute,
                            ));
                        }

                        client_account.available -= transaction.amount;
                        client_account.held += transaction.amount;
                    }
                    AmendmentType::Resolve => {
                        if !self.in_dispute.remove(&amendment.transaction_id) {
                            return Err(Error::Processing(
                                ProcessingError::ResolvedTransactionWasNotInDispute,
                            ));
                        }

                        client_account.available += transaction.amount;
                        client_account.held -= transaction.amount;
                    }
                    AmendmentType::Chargeback => {
                        if !self.in_dispute.remove(&amendment.transaction_id) {
                            return Err(Error::Processing(
                                ProcessingError::ChargedBackWasNotInDispute,
                            ));
                        }

                        client_account.held -= transaction.amount;
                        client_account.locked = true;
                    }
                }

                assert!(
                    client_account.held >= Decimal::zero(),
                    "We don't expect amount held to go negative in any scenario"
                );
                self.accounts.insert(amendment.client_id, client_account);
                Ok(())
            }
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
        eprintln!("Got file {}", &filename); // TODO: replace with log trace

        let csv_records = CsvReader::new(&std::path::Path::new(&filename))?;
        let mut transaction_processor = TransactionProcessor::default();
        for record in csv_records.into_iter() {
            if let Err(err) = transaction_processor.process(&record) {
                eprintln!("Transaction [{:?}] processing error: {:?}", &record, &err);
            }
        }

        let stdout = std::io::stdout();
        let stdout_lock = stdout.lock();
        let mut csv_writer = csv::Writer::from_writer(stdout_lock);
        for (client_id, account) in transaction_processor.accounts.iter() {
            csv_writer.serialize(AccountRecord { client_id, account })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use rust_decimal_macros::dec;

    #[derive(Default)]
    struct TransactionGenerator {
        transaction_count: u32,
        clients_of_transactions: std::collections::HashMap<TransactionID, ClientID>,
    }

    impl TransactionGenerator {
        fn adjust_amount(&mut self, client_id: ClientID, amount: Decimal) -> Record {
            assert_ne!(amount, dec!(0), "We don't expect zero amount transactions");

            self.transaction_count += 1;
            let transaction_id = TransactionID {
                id: self.transaction_count,
            };
            let transaction_type = if amount < dec!(0) {
                TransactionType::Withdrawal
            } else {
                TransactionType::Deposit
            };

            self.clients_of_transactions
                .insert(transaction_id, client_id);
            Record::Transaction(NewTransaction {
                transaction_id,
                client_id,
                amount: amount.abs(),
                transaction_type,
            })
        }

        fn dispute(&mut self, transaction_id: TransactionID) -> Record {
            let client_id = *self
                .clients_of_transactions
                .get(&transaction_id)
                .expect("Unknown transaction");
            Record::Amendment(TransactionAmendment {
                client_id,
                transaction_id,
                amendment_type: AmendmentType::Dispute,
            })
        }

        fn resolve(&mut self, transaction_id: TransactionID) -> Record {
            let client_id = *self
                .clients_of_transactions
                .get(&transaction_id)
                .expect("Unknown transaction");
            Record::Amendment(TransactionAmendment {
                client_id,
                transaction_id,
                amendment_type: AmendmentType::Resolve,
            })
        }
    }

    #[test]
    fn test_excessive_withdrawal() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        assert!(processor
            .process(&generator.adjust_amount(ClientID::new(23), dec!(2)))
            .is_ok());
        assert!(processor
            .process(&generator.adjust_amount(ClientID::new(23), dec!(-3)))
            .is_err());

        assert_eq!(
            processor
                .accounts
                .get(&ClientID::new(23))
                .unwrap()
                .available,
            dec!(2)
        )
    }

    #[test]
    fn test_dispute() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let client_id = ClientID::new(23);
        let deposit = generator.adjust_amount(client_id, dec!(10));
        let withdrawal = generator.adjust_amount(client_id, dec!(-7));
        assert!(processor.process(&deposit).is_ok());
        assert!(processor.process(&withdrawal).is_ok());

        let initial_state = processor.accounts.get(&client_id).cloned();
        assert!(initial_state.is_some(), "Client account must exist");

        // Dispute with wrong client id is rejected and doesn't change the state
        let dispute_with_wrong_client = Record::Amendment(TransactionAmendment {
            amendment_type: AmendmentType::Dispute,
            client_id: ClientID::new(72),
            transaction_id: deposit.transaction_id(),
        });

        assert!(processor.process(&dispute_with_wrong_client).is_err());
        assert_eq!(processor.accounts.get(&client_id).cloned(), initial_state);

        // Dispute with unknown transaction id is rejected and doesn't change the state
        let dispute_of_non_existent_transaction = Record::Amendment(TransactionAmendment {
            amendment_type: AmendmentType::Dispute,
            client_id,
            transaction_id: TransactionID::new(42),
        });

        assert!(processor
            .process(&dispute_of_non_existent_transaction)
            .is_err());
        assert_eq!(processor.accounts.get(&client_id).cloned(), initial_state);

        // Dispute is handled as expected
        let deposit_dispute = generator.dispute(deposit.transaction_id());
        assert!(processor.process(&deposit_dispute).is_ok());

        let state_in_dispute = processor.accounts.get(&client_id).unwrap().clone();
        assert_eq!(state_in_dispute.available, dec!(-7));
        assert_eq!(state_in_dispute.held, dec!(10));
        assert!(!state_in_dispute.locked);

        // Double dispute is rejected and state remains the same
        assert!(processor.process(&deposit_dispute).is_err());
        let state_after_double_dispute = processor.accounts.get(&client_id).unwrap().clone();
        assert_eq!(state_in_dispute, state_after_double_dispute);

        // Having two transactions in dispute is okay and reflects correctly on the client account
        let withdrawal_dispute = generator.dispute(withdrawal.transaction_id());
        assert!(processor.process(&withdrawal_dispute).is_ok());
        let state_with_two_disputes = processor.accounts.get(&client_id).unwrap().clone();
        // Initial total was 3, disputing 17 brings us to -14
        assert_eq!(state_with_two_disputes.available, dec!(-14));
        assert_eq!(state_with_two_disputes.held, dec!(17));
        assert!(!state_with_two_disputes.locked);
    }

    #[test]
    fn test_resolve() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let client_id = ClientID::new(23);
        let deposit = generator.adjust_amount(client_id, dec!(10));
        let withdrawal = generator.adjust_amount(client_id, dec!(-7));
        assert!(processor.process(&deposit).is_ok());
        assert!(processor.process(&withdrawal).is_ok());

        let initial_state = processor.accounts.get(&client_id).unwrap().clone();

        let dispute_deposit = generator.dispute(deposit.transaction_id());
        let resolve_deposit = generator.resolve(deposit.transaction_id());

        // Resolving a transaction that is not in dispute is not alllowed
        assert!(processor.process(&resolve_deposit).is_err());

        // Resolving a disputed transaction works as expected
        assert!(processor.process(&dispute_deposit).is_ok());
        assert!(processor.process(&resolve_deposit).is_ok());
        assert_eq!(initial_state, *processor.accounts.get(&client_id).unwrap());

        // Second resolve of the same transaction is an error and doesn't change the state
        assert!(processor.process(&resolve_deposit).is_err());
        assert_eq!(initial_state, *processor.accounts.get(&client_id).unwrap());
    }
}
