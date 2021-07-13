use log::{error, warn};
use std::convert::TryFrom;

use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum InputFormatError {
    MissingAmount,
    NegativeAmount,
    CsvError(csv::Error),
}

impl std::convert::From<csv::Error> for InputFormatError {
    fn from(csv_error: csv::Error) -> InputFormatError {
        InputFormatError::CsvError(csv_error)
    }
}

impl std::fmt::Display for InputFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for InputFormatError {}

#[derive(Debug)]
pub enum ProcessingError {
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct TransactionID {
    id: u32,
}

impl TransactionID {
    pub fn new(id: u32) -> TransactionID {
        TransactionID { id }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ClientID {
    id: u16,
}

impl ClientID {
    pub fn new(id: u16) -> ClientID {
        ClientID { id }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferType {
    Deposit,
    Withdrawal,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Transfer {
    #[serde(alias = "type")]
    pub transfer_type: TransferType,
    #[serde(alias = "client")]
    pub client_id: ClientID,
    #[serde(alias = "tx")]
    pub transaction_id: TransactionID,
    pub amount: Decimal,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AmendmentType {
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Amendment {
    #[serde(alias = "type")]
    pub amendment_type: AmendmentType,
    #[serde(alias = "client")]
    pub client_id: ClientID,
    #[serde(alias = "tx")]
    pub transaction_id: TransactionID,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Transaction {
    Transfer(Transfer),
    Amendment(Amendment),
}

impl Transaction {
    pub fn transaction_id(&self) -> TransactionID {
        match self {
            Transaction::Transfer(transfer) => transfer.transaction_id,
            Transaction::Amendment(amendment) => amendment.transaction_id,
        }
    }
}

impl std::fmt::Display for Transfer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?}, client_id : {}, transaction_id : {}, amount : {}",
            self.transfer_type, self.client_id.id, self.transaction_id.id, self.amount
        )
    }
}

impl std::fmt::Display for Amendment {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?}, client_id : {}, transaction_id : {}",
            self.amendment_type, self.client_id.id, self.transaction_id.id
        )
    }
}

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Transaction::Transfer(transfer) => transfer.fmt(f),
            Transaction::Amendment(amendment) => amendment.fmt(f),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum TransactionType {
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
                    warn!(
                        "Amount on transation {:?} will be ignored. You can only dispute the entire transfer and can't alter the amount being disputed",
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

pub struct CsvReader<CsvInput: std::io::Read> {
    csv_reader: csv::Reader<CsvInput>,
}

impl CsvReader<std::fs::File> {
    pub fn from_path(filepath: &std::path::Path) -> Result<Self, std::io::Error> {
        Ok(CsvReader::from_reader(std::fs::File::open(filepath)?))
    }
}

impl<CsvInput: std::io::Read> CsvReader<CsvInput> {
    pub fn from_reader(input: CsvInput) -> Self {
        Self {
            csv_reader: csv::ReaderBuilder::new()
                .trim(csv::Trim::All)
                .flexible(true)
                .from_reader(input),
        }
    }

    fn get_next_transaction(&mut self) -> Option<Result<Transaction, InputFormatError>> {
        self.csv_reader
            .deserialize::<RawTransaction>()
            .next()
            .map(|result| {
                result
                    .map_err(InputFormatError::from)
                    .and_then(Transaction::try_from)
            })
    }
}

impl<CsvInput: std::io::Read> std::iter::Iterator for CsvReader<CsvInput> {
    type Item = Transaction;

    fn next(&mut self) -> Option<Transaction> {
        while let Some(result) = self.get_next_transaction() {
            match result {
                Ok(transaction) => return Some(transaction),
                Err(err) => error!("CSV parsing error: {:?}", &err),
            }
        }
        None
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Account {
    pub available: Decimal,
    pub held: Decimal,
    pub locked: bool,
}

impl Account {
    fn total(&self) -> Decimal {
        self.available + self.held
    }
}

#[derive(Debug, Clone)]
pub struct AccountWithClientID<'a> {
    pub client_id: &'a ClientID,
    pub account: &'a Account,
}

impl<'a> Serialize for AccountWithClientID<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("AccountWithClientID", 5)?;
        state.serialize_field("client", &self.client_id)?;
        state.serialize_field("available", &self.account.available)?;
        state.serialize_field("held", &self.account.held)?;
        state.serialize_field("total", &self.account.total())?;
        state.serialize_field("locked", &self.account.locked)?;
        state.end()
    }
}

#[derive(Default)]
pub struct TransactionProcessor {
    pub accounts: std::collections::HashMap<ClientID, Account>,
    transfers: std::collections::HashMap<TransactionID, Transfer>,
    in_dispute: std::collections::HashSet<TransactionID>,
    charged_back: std::collections::HashSet<TransactionID>,
}

impl TransactionProcessor {
    pub fn process(&mut self, transaction: &Transaction) -> Result<(), ProcessingError> {
        match transaction {
            Transaction::Transfer(transfer) => {
                if self.transfers.contains_key(&transfer.transaction_id) {
                    return Err(ProcessingError::TransactionIdAlreadyExists);
                }
                let mut client_account = self
                    .accounts
                    .get(&transfer.client_id)
                    .cloned()
                    .unwrap_or_default();

                if client_account.locked {
                    return Err(ProcessingError::TransferOnLockedAccount);
                }
                match transfer.transfer_type {
                    TransferType::Deposit => client_account.available += transfer.amount,
                    TransferType::Withdrawal => {
                        if client_account.available >= transfer.amount {
                            client_account.available -= transfer.amount;
                        } else {
                            return Err(ProcessingError::NotEnoughMoneyForWithdrawal);
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
                    None => return Err(ProcessingError::TryingToDisputeUnknownTransaction),
                };
                if transfer.client_id != amendment.client_id {
                    return Err(ProcessingError::WrongClientInDispute);
                }

                let mut client_account = self
                    .accounts
                    .get(&amendment.client_id)
                    .cloned()
                    .expect("Client account must be present for recognised transactions");

                match amendment.amendment_type {
                    AmendmentType::Dispute => {
                        if !self.in_dispute.insert(amendment.transaction_id) {
                            return Err(ProcessingError::TransferIsAlreadyInDispute);
                        }
                        if self.charged_back.contains(&amendment.transaction_id) {
                            return Err(ProcessingError::DisputingAlreadyChargedBackTransfer);
                        }

                        client_account.available -= transfer.amount;
                        client_account.held += transfer.amount;
                    }
                    AmendmentType::Resolve => {
                        if !self.in_dispute.remove(&amendment.transaction_id) {
                            return Err(ProcessingError::ResolvedTransferWasNotInDispute);
                        }

                        client_account.available += transfer.amount;
                        client_account.held -= transfer.amount;
                    }
                    AmendmentType::Chargeback => {
                        if !self.in_dispute.remove(&amendment.transaction_id) {
                            return Err(ProcessingError::ChargedBackTransferWasNotInDispute);
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
