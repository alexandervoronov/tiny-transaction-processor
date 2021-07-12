use std::convert::TryFrom;

use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
enum InputFormatError {
    MissingAmount,
    NegativeAmount,
}

impl std::fmt::Display for InputFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for InputFormatError {}

#[derive(Debug)]
enum ProcessingError {
    TransferOnLockedAccount,
    NotEnoughMoneyForWithdrawal,
    TryingToDisputeUnknownTransaction,
    WrongClientInDispute,
    TransferIsAlreadyInDispute,
    ResolvedTransferWasNotInDispute,
    ChargedBackTransferWasNotInDispute,
    DisputingAlreadyChargedBackTransfer,
    TransactionIdAlreadyExists,
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

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum TransferType {
    Deposit,
    Withdrawal,
}

// TODO: rename to transfer
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct Transfer {
    #[serde(alias = "type")]
    transfer_type: TransferType,
    #[serde(alias = "client")]
    client_id: ClientID,
    #[serde(alias = "tx")]
    transaction_id: TransactionID,
    amount: Decimal,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum AmendmentType {
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct Amendment {
    #[serde(alias = "type")]
    amendment_type: AmendmentType,
    #[serde(alias = "client")]
    client_id: ClientID,
    #[serde(alias = "tx")]
    transaction_id: TransactionID,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum Transaction {
    Transfer(Transfer),
    Amendment(Amendment),
}

impl Transaction {
    #[cfg(test)]
    fn transaction_id(&self) -> TransactionID {
        match self {
            Transaction::Transfer(transfer) => transfer.transaction_id,
            Transaction::Amendment(amendment) => amendment.transaction_id,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum TransactionType {
    Transfer(TransferType),
    Amendment(AmendmentType),
}

#[derive(Debug, Deserialize)]
struct RawTransaction {
    #[serde(alias = "type")]
    transaction_type: TransactionType,
    #[serde(alias = "client")]
    client_id: ClientID,
    #[serde(alias = "tx")]
    transaction_id: TransactionID,
    amount: Option<Decimal>,
}

impl std::convert::TryFrom<RawTransaction> for Transaction {
    type Error = InputFormatError;
    fn try_from(transaction: RawTransaction) -> Result<Transaction, Self::Error> {
        match transaction.transaction_type {
            TransactionType::Amendment(amendment_type) => {
                if transaction.amount.is_some() {
                    eprintln!(
                        "Warning: amount info on transation {:?} will be ignored",
                        &transaction
                    );
                }
                Ok(Transaction::Amendment(Amendment {
                    amendment_type,
                    client_id: transaction.client_id,
                    transaction_id: transaction.transaction_id,
                }))
            }
            TransactionType::Transfer(transfer_type) => match transaction.amount {
                Some(amount) => {
                    if amount < Decimal::zero() {
                        Err(InputFormatError::NegativeAmount)
                    } else {
                        Ok(Transaction::Transfer(Transfer {
                            transfer_type,
                            amount,
                            client_id: transaction.client_id,
                            transaction_id: transaction.transaction_id,
                        }))
                    }
                }
                None => Err(InputFormatError::MissingAmount),
            },
        }
    }
}

struct CsvReader<CsvInput: std::io::Read> {
    csv_reader: csv::Reader<CsvInput>,
}

impl CsvReader<std::fs::File> {
    fn from_path(filepath: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(CsvReader::from_reader(std::fs::File::open(filepath)?))
    }
}

impl<CsvInput: std::io::Read> CsvReader<CsvInput> {
    fn from_reader(input: CsvInput) -> Self {
        Self {
            csv_reader: csv::ReaderBuilder::new()
                .trim(csv::Trim::All)
                .flexible(true)
                .from_reader(input),
        }
    }

    fn get_next_transaction(&mut self) -> Result<Option<Transaction>, Box<dyn std::error::Error>> {
        let next_transaction = self
            .csv_reader
            .deserialize::<RawTransaction>()
            .next()
            .transpose()?;
        next_transaction
            .map(Transaction::try_from)
            .transpose()
            .map_err(|err| err.into())
    }
}

impl<CsvInput: std::io::Read> std::iter::Iterator for CsvReader<CsvInput> {
    type Item = Transaction;

    // TODO: don't fail on the first error and eat out the rest of the file
    fn next(self: &mut Self) -> Option<Transaction> {
        self.get_next_transaction()
            .map_err(|err| eprintln!("CSV parsing error: {:?}", &err))
            .ok()
            .flatten()
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
struct AccountWithClientID<'a> {
    client_id: &'a ClientID,
    account: &'a Account,
}

impl<'a> Serialize for AccountWithClientID<'a> {
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
    transfers: std::collections::HashMap<TransactionID, Transfer>,
    accounts: std::collections::HashMap<ClientID, Account>,
    in_dispute: std::collections::HashSet<TransactionID>,
    charged_back: std::collections::HashSet<TransactionID>,
}

impl TransactionProcessor {
    // TODO: can return processing error
    fn process(&mut self, transaction: &Transaction) -> Result<(), Error> {
        match transaction {
            Transaction::Transfer(transfer) => {
                if self.transfers.contains_key(&transfer.transaction_id) {
                    return Err(Error::Processing(
                        ProcessingError::TransactionIdAlreadyExists,
                    ));
                }
                let mut client_account = self
                    .accounts
                    .get(&transfer.client_id)
                    .cloned()
                    .unwrap_or_default();

                if client_account.locked {
                    return Err(Error::Processing(ProcessingError::TransferOnLockedAccount));
                }
                match transfer.transfer_type {
                    TransferType::Deposit => client_account.available += transfer.amount,
                    TransferType::Withdrawal => {
                        if client_account.available >= transfer.amount {
                            client_account.available -= transfer.amount;
                        } else {
                            return Err(Error::Processing(
                                ProcessingError::NotEnoughMoneyForWithdrawal,
                            ));
                        }
                    }
                }
                self.accounts.insert(transfer.client_id, client_account);
                self.transfers
                    .insert(transfer.transaction_id, transfer.clone());
                Ok(())
            }
            Transaction::Amendment(amendment) => {
                let transfer = match self.transfers.get(&amendment.transaction_id) {
                    Some(transfer) => transfer,
                    None => {
                        return Err(Error::Processing(
                            ProcessingError::TryingToDisputeUnknownTransaction,
                        ))
                    }
                };
                if transfer.client_id != amendment.client_id {
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
                                ProcessingError::TransferIsAlreadyInDispute,
                            ));
                        }
                        if self.charged_back.contains(&amendment.transaction_id) {
                            return Err(Error::Processing(
                                ProcessingError::DisputingAlreadyChargedBackTransfer,
                            ));
                        }

                        client_account.available -= transfer.amount;
                        client_account.held += transfer.amount;
                    }
                    AmendmentType::Resolve => {
                        if !self.in_dispute.remove(&amendment.transaction_id) {
                            return Err(Error::Processing(
                                ProcessingError::ResolvedTransferWasNotInDispute,
                            ));
                        }

                        client_account.available += transfer.amount;
                        client_account.held -= transfer.amount;
                    }
                    AmendmentType::Chargeback => {
                        if !self.in_dispute.remove(&amendment.transaction_id) {
                            return Err(Error::Processing(
                                ProcessingError::ChargedBackTransferWasNotInDispute,
                            ));
                        }

                        client_account.held -= transfer.amount;
                        client_account.locked = true;
                        self.charged_back.insert(amendment.transaction_id);
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
        fn transfer(&mut self, client_id: ClientID, amount: Decimal) -> Transaction {
            assert_ne!(amount, dec!(0), "We don't expect zero amount transactions");

            self.transaction_count += 1;
            let transaction_id = TransactionID {
                id: self.transaction_count,
            };
            let transfer_type = if amount < dec!(0) {
                TransferType::Withdrawal
            } else {
                TransferType::Deposit
            };

            self.clients_of_transactions
                .insert(transaction_id, client_id);
            Transaction::Transfer(Transfer {
                transaction_id,
                client_id,
                amount: amount.abs(),
                transfer_type,
            })
        }

        fn dispute(&mut self, transaction_id: TransactionID) -> Transaction {
            let client_id = *self
                .clients_of_transactions
                .get(&transaction_id)
                .expect("Unknown transaction");
            Transaction::Amendment(Amendment {
                client_id,
                transaction_id,
                amendment_type: AmendmentType::Dispute,
            })
        }

        fn resolve(&mut self, transaction_id: TransactionID) -> Transaction {
            let client_id = *self
                .clients_of_transactions
                .get(&transaction_id)
                .expect("Unknown transaction");
            Transaction::Amendment(Amendment {
                client_id,
                transaction_id,
                amendment_type: AmendmentType::Resolve,
            })
        }

        fn chargeback(&mut self, transaction_id: TransactionID) -> Transaction {
            let client_id = *self
                .clients_of_transactions
                .get(&transaction_id)
                .expect("Unknown transaction");
            Transaction::Amendment(Amendment {
                client_id,
                transaction_id,
                amendment_type: AmendmentType::Chargeback,
            })
        }
    }

    #[test]
    fn test_excessive_withdrawal() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let client_id = ClientID::new(23);
        assert!(processor
            .process(&generator.transfer(client_id, dec!(2)))
            .is_ok());
        assert!(processor
            .process(&generator.transfer(client_id, dec!(-3)))
            .is_err());

        assert_eq!(
            processor.accounts.get(&client_id).unwrap().available,
            dec!(2)
        )
    }

    #[test]
    fn test_dispute() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let client_id = ClientID::new(23);
        let deposit = generator.transfer(client_id, dec!(10));
        let withdrawal = generator.transfer(client_id, dec!(-7));
        assert!(processor.process(&deposit).is_ok());
        assert!(processor.process(&withdrawal).is_ok());

        let initial_state = processor.accounts.get(&client_id).cloned();
        assert!(initial_state.is_some(), "Client account must exist");

        // Dispute with wrong client id is rejected and doesn't change the state
        let dispute_with_wrong_client = Transaction::Amendment(Amendment {
            amendment_type: AmendmentType::Dispute,
            client_id: ClientID::new(72),
            transaction_id: deposit.transaction_id(),
        });

        assert!(processor.process(&dispute_with_wrong_client).is_err());
        assert_eq!(processor.accounts.get(&client_id).cloned(), initial_state);

        // Dispute with unknown transaction id is rejected and doesn't change the state
        let dispute_of_non_existent_transaction = Transaction::Amendment(Amendment {
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
        let deposit = generator.transfer(client_id, dec!(10));
        let withdrawal = generator.transfer(client_id, dec!(-7));
        assert!(processor.process(&deposit).is_ok());
        assert!(processor.process(&withdrawal).is_ok());

        let initial_state = processor.accounts.get(&client_id).unwrap().clone();

        let dispute_deposit = generator.dispute(deposit.transaction_id());
        let resolve_deposit = generator.resolve(deposit.transaction_id());

        // Resolving a transaction that is not in dispute is not alllowed
        assert!(processor.process(&resolve_deposit).is_err());
        assert_eq!(initial_state, *processor.accounts.get(&client_id).unwrap());

        // Resolving a disputed transaction works as expected
        assert!(processor.process(&dispute_deposit).is_ok());
        assert!(processor.process(&resolve_deposit).is_ok());
        assert_eq!(initial_state, *processor.accounts.get(&client_id).unwrap());

        // Second resolve of the same transaction is an error and doesn't change the state
        assert!(processor.process(&resolve_deposit).is_err());
        assert_eq!(initial_state, *processor.accounts.get(&client_id).unwrap());
    }

    #[test]
    fn test_chargeback() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let client_id = ClientID::new(23);
        let deposit = generator.transfer(client_id, dec!(10));
        let withdrawal = generator.transfer(client_id, dec!(-7));
        assert!(processor.process(&deposit).is_ok());
        assert!(processor.process(&withdrawal).is_ok());

        let initial_state = processor.accounts.get(&client_id).unwrap().clone();

        let dispute_deposit = generator.dispute(deposit.transaction_id());
        let chargeback_deposit = generator.chargeback(deposit.transaction_id());

        // Charging back a transaction that is not in dispute is not allowed
        assert!(processor.process(&chargeback_deposit).is_err());
        assert_eq!(initial_state, *processor.accounts.get(&client_id).unwrap());

        // Charging back a transaction in dispute works as expected
        assert!(processor.process(&dispute_deposit).is_ok());
        assert!(processor.process(&chargeback_deposit).is_ok());
        let state_after_chargeback = processor.accounts.get(&client_id).unwrap().clone();
        assert_eq!(state_after_chargeback.available, dec!(-7));
        assert_eq!(state_after_chargeback.held, Decimal::zero());
        assert!(state_after_chargeback.locked);

        // Re-disputing a charged back transaction is not allowed to prevent double chargeback
        assert!(processor.process(&dispute_deposit).is_err());
        assert_eq!(
            state_after_chargeback,
            *processor.accounts.get(&client_id).unwrap()
        );

        // Disputing and charging back other transactions still works as expected
        let dispute_withdrawal = generator.dispute(withdrawal.transaction_id());
        let chargeback_withdrawal = generator.chargeback(withdrawal.transaction_id());
        assert!(processor.process(&dispute_withdrawal).is_ok());
        assert!(processor.process(&chargeback_withdrawal).is_ok());
        let state_after_both_chargebacks = processor.accounts.get(&client_id).unwrap().clone();
        assert_eq!(state_after_both_chargebacks.available, dec!(-14));
        assert_eq!(state_after_both_chargebacks.held, Decimal::zero());
        assert!(state_after_both_chargebacks.locked);
    }

    #[test]
    fn test_locked() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let client_id = ClientID::new(23);
        let locked_account = Account {
            available: dec!(15),
            held: Decimal::zero(),
            locked: true,
        };
        processor.accounts.insert(client_id, locked_account.clone());

        // Trying to deposit or withdraw from a locked account fails
        let deposit = generator.transfer(client_id, dec!(10));
        assert!(processor.process(&deposit).is_err());
        assert_eq!(locked_account, *processor.accounts.get(&client_id).unwrap());

        let withdrawal = generator.transfer(client_id, dec!(-7));
        assert!(processor.process(&withdrawal).is_err());
        assert_eq!(locked_account, *processor.accounts.get(&client_id).unwrap());

        // Disputing of the failed transactions also fails
        assert!(processor
            .process(&generator.dispute(deposit.transaction_id()))
            .is_err());
        assert!(processor
            .process(&generator.dispute(withdrawal.transaction_id()))
            .is_err());
    }

    #[test]
    fn test_disputing_a_failed_withdrawal() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let client_id = ClientID::new(23);
        let initial_state = Account {
            available: dec!(15),
            ..Account::default()
        };
        processor.accounts.insert(client_id, initial_state.clone());

        // Trying to deposit or withdraw from a locked account fails
        let excessive_withdrawal = generator.transfer(client_id, dec!(-16));
        assert!(processor.process(&excessive_withdrawal).is_err());

        let dispute = generator.dispute(excessive_withdrawal.transaction_id());
        assert!(processor.process(&dispute).is_err());
        assert_eq!(initial_state, *processor.accounts.get(&client_id).unwrap());
    }

    #[test]
    fn transaction_with_the_same_id_isnt_allowed() {
        let mut generator = TransactionGenerator::default();
        let mut processor = TransactionProcessor::default();

        let good_client_id = ClientID::new(23);
        let good_deposit = generator.transfer(good_client_id, dec!(10));

        let weird_client_id = ClientID::new(243);
        let weird_deposit_reusing_transaction_id = Transaction::Transfer(Transfer {
            client_id: weird_client_id,
            transaction_id: good_deposit.transaction_id(),
            transfer_type: TransferType::Deposit,
            amount: dec!(10),
        });

        assert!(processor.process(&good_deposit).is_ok());

        // Can't redo the same transaction
        assert!(processor.process(&good_deposit).is_err());
        assert_eq!(
            processor.accounts.get(&good_client_id).unwrap().available,
            dec!(10)
        );

        // Can't use the transaction ID for a different client too
        assert!(processor
            .process(&weird_deposit_reusing_transaction_id)
            .is_err());

        // Even after chargeback still can't reuse the transaction ID
        let dispute_good_deposit = generator.dispute(good_deposit.transaction_id());
        let chargeback_good_deposit = generator.chargeback(good_deposit.transaction_id());
        assert!(processor.process(&dispute_good_deposit).is_ok());
        assert!(processor.process(&chargeback_good_deposit).is_ok());

        assert!(processor
            .process(&weird_deposit_reusing_transaction_id)
            .is_err());
    }

    #[test]
    fn test_csv_parsing_and_processing() {
        let input_csv = r#"type, client, tx, amount
            deposit,      1,  1,    0.0010
            deposit,      1,  2,    0.0020
            deposit,      1,  3,    0.0030
            withdrawal,   1,  4,    0.0050
            deposit,      2,  5,    12.0
            withdrawal,   2,  6,    40.0
            dispute,      2,  5
        "#;
        let csv_reader = CsvReader::from_reader(input_csv.as_bytes());

        let mut processor = TransactionProcessor::default();
        for record in csv_reader {
            processor.process(&record).ok();
        }

        assert_eq!(
            *processor.accounts.get(&ClientID::new(1)).unwrap(),
            Account {
                available: dec!(0.001),
                ..Default::default()
            }
        );

        assert_eq!(
            *processor.accounts.get(&ClientID::new(2)).unwrap(),
            Account {
                available: Decimal::zero(),
                held: dec!(12),
                locked: false
            }
        );
    }

    fn get_transactions(input_csv: &str) -> Vec<Transaction> {
        CsvReader::from_reader(input_csv.as_bytes()).collect::<Vec<_>>()
    }

    fn extract_type(record: &Transaction) -> TransactionType {
        match record {
            Transaction::Transfer(transaction) => {
                TransactionType::Transfer(transaction.transfer_type)
            }
            Transaction::Amendment(amendment) => {
                TransactionType::Amendment(amendment.amendment_type)
            }
        }
    }

    #[test]
    fn test_csv_parsing_happy_cases() {
        let input_csv = r#"type, client, tx, amount
            deposit   , 1,  1, 10,
            withdrawal, 1,  2, 20,
            dispute   , 2,  4
            resolve   , 3,  5
            chargeback, 4, 10
        "#;

        let transactions = get_transactions(input_csv);

        assert_eq!(transactions.len(), 5);
        assert_eq!(
            transactions[0],
            Transaction::Transfer(Transfer {
                transfer_type: TransferType::Deposit,
                amount: dec!(10),
                client_id: ClientID::new(1),
                transaction_id: TransactionID::new(1)
            })
        );
        assert_eq!(
            transactions[1],
            Transaction::Transfer(Transfer {
                transfer_type: TransferType::Withdrawal,
                amount: dec!(20),
                client_id: ClientID::new(1),
                transaction_id: TransactionID::new(2)
            })
        );
        assert_eq!(
            transactions[2],
            Transaction::Amendment(Amendment {
                amendment_type: AmendmentType::Dispute,
                client_id: ClientID::new(2),
                transaction_id: TransactionID::new(4)
            })
        );
        assert_eq!(
            transactions[3],
            Transaction::Amendment(Amendment {
                amendment_type: AmendmentType::Resolve,
                client_id: ClientID::new(3),
                transaction_id: TransactionID::new(5)
            })
        );
        assert_eq!(
            transactions[4],
            Transaction::Amendment(Amendment {
                amendment_type: AmendmentType::Chargeback,
                client_id: ClientID::new(4),
                transaction_id: TransactionID::new(10)
            })
        );
    }

    #[test]
    fn test_csv_parsing_tricky_cases() {
        let header_only_csv = "type, client, tx, amount";
        assert!(get_transactions(header_only_csv).is_empty());
        let header_with_line_break_csv = "type, client, tx, amount\n";
        assert!(get_transactions(header_with_line_break_csv).is_empty());

        let trailing_comma_transactions_csv = r#"type, client, tx, amount
            deposit, 1, 1, 1.0,
            dispute, 1, 1, 
            resolve, 1, 1, ,"#;
        let trailing_comma_transactions = get_transactions(trailing_comma_transactions_csv);
        assert_eq!(trailing_comma_transactions.len(), 3);
        assert_eq!(
            trailing_comma_transactions
                .iter()
                .map(extract_type)
                .collect::<Vec<_>>(),
            vec![
                TransactionType::Transfer(TransferType::Deposit),
                TransactionType::Amendment(AmendmentType::Dispute),
                TransactionType::Amendment(AmendmentType::Resolve)
            ]
        );

        let trailing_comma_header_csv = r#"type, client, tx, amount,
            deposit, 1, 1, 1.0"#;
        let trailing_comma_header_transactions = get_transactions(trailing_comma_header_csv);
        assert!(trailing_comma_header_transactions.is_empty());

        let dispute_with_amount_csv = r#"type, client, tx, amount
            dispute, 3, 5, 8"#;
        let dispute_with_amount_transactions = get_transactions(dispute_with_amount_csv);
        assert_eq!(dispute_with_amount_transactions.len(), 1);
        assert_eq!(
            dispute_with_amount_transactions[0],
            Transaction::Amendment(Amendment {
                amendment_type: AmendmentType::Dispute,
                client_id: ClientID::new(3),
                transaction_id: TransactionID::new(5)
            })
        );

        let withdrawal_without_amount_csv = r#"type, client, tx, amount
            withdrawal, 1, 2"#;
        assert!(get_transactions(withdrawal_without_amount_csv).is_empty());

        let negative_amount_csv = r#"type, client, tx, amount
            deposit, 1, 2, -3"#;
        assert!(get_transactions(negative_amount_csv).is_empty());

        let invalid_client_id_csv = r#"type, client, tx, amount
        deposit, banana, 2, -3"#;
        assert!(get_transactions(invalid_client_id_csv).is_empty());

        // Currently we error on the first line with invalid formatting. We should be able to do better, but let's
        // say this is good enough for now.
        let csv_with_invalid_entry = r#"type, client, tx, amount
            deposit, 1, 1, 12
            banana
            withdrawal, 1, 2, 10
        "#;
        let transactions_with_invalid_entry = get_transactions(csv_with_invalid_entry);
        assert_eq!(transactions_with_invalid_entry.len(), 1);
        assert_eq!(
            extract_type(&transactions_with_invalid_entry[0]),
            TransactionType::Transfer(TransferType::Deposit)
        );
    }
}
