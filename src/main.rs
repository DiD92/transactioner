use std::collections::{hash_map::Entry::Occupied, hash_map::Entry::Vacant, HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fmt;

use serde::{Deserialize, Deserializer};

type ClientId = u16;
type TransactionsWithAccounts = (Vec<Transaction>, HashMap<ClientId, ClientAccount>);

fn transaction_type_deserializer<'de, D>(deserializer: D) -> Result<TransactionType, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    match buf.as_str() {
        "deposit" => Ok(TransactionType::Deposit),
        "withdrawal" => Ok(TransactionType::Withdrawal),
        "dispute" => Ok(TransactionType::Dispute),
        "resolve" => Ok(TransactionType::Resolve),
        "chargeback" => Ok(TransactionType::Chargeback),
        _ => Ok(TransactionType::Unknown),
    }
}

#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(deserialize_with = "transaction_type_deserializer")]
    r#type: TransactionType,
    client: ClientId,
    tx: u32,
    amount: f32,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[repr(u8)]
enum TransactionType {
    Deposit = 0,
    Withdrawal = 1,
    Dispute = 2,
    Resolve = 4,
    Chargeback = 8,
    Unknown = 16,
}

#[derive(Debug, Default)]
struct ClientAccount {
    client: ClientId,
    available: f32,
    held: f32,
    locked: bool,
    transactions: HashMap<u32, f32>,
    disputed_transactions: HashSet<u32>,
}

impl ClientAccount {
    pub fn new(client: ClientId) -> Self {
        ClientAccount {
            client,
            ..Default::default()
        }
    }

    pub fn apply_transaction(&mut self, transaction: Transaction) {
        // If the transaction doesn't belong to this account
        // or the account is locked, we skip it.
        if transaction.client != self.client || self.locked {
            return;
        }

        match transaction.r#type {
            TransactionType::Deposit => {
                self.available += transaction.amount;
                self.transactions.insert(transaction.tx, transaction.amount);
            }
            TransactionType::Withdrawal => {
                if self.available >= transaction.amount {
                    self.available -= transaction.amount;
                }
            }
            TransactionType::Dispute => {
                if let Occupied(entry) = self.transactions.entry(transaction.tx) {
                    let held_amount = entry.get();
                    // If there are not enough funds to hold
                    // we consider the dispute erroneous
                    // because the disputed funds have already
                    // been withdrawn by a previous transaction
                    if self.available >= *held_amount {
                        self.available -= held_amount;
                        self.held += held_amount;
                        self.disputed_transactions.insert(transaction.tx);
                    }
                }
            }
            TransactionType::Resolve => {
                if let Occupied(entry) = self.transactions.entry(transaction.tx) {
                    if self.disputed_transactions.contains(entry.key()) {
                        let held_amount = entry.get();
                        self.available += held_amount;
                        self.held -= held_amount;
                        self.disputed_transactions.remove(entry.key());
                    }
                }
            }
            TransactionType::Chargeback => {
                if let Occupied(entry) = self.transactions.entry(transaction.tx) {
                    if self.disputed_transactions.contains(entry.key()) {
                        let held_amount = entry.get();
                        self.held -= held_amount;
                        self.disputed_transactions.remove(entry.key());
                        self.locked = true;
                    }
                }
            }
            TransactionType::Unknown => {
                // If a transaction record was malformed, we ignore it.
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct ClientState {
    client: ClientId,
    available: f32,
    held: f32,
    locked: bool,
}

impl From<ClientAccount> for ClientState {
    fn from(ca: ClientAccount) -> Self {
        ClientState {
            client: ca.client,
            available: ca.available,
            held: ca.held,
            locked: ca.locked,
        }
    }
}

impl fmt::Display for ClientState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{:.4},{:.4},{:.4},{}",
            self.client,
            self.available,
            self.held,
            self.available + self.held,
            self.locked
        )
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Missing input file, exiting...")
    } else {
        let file_path = &args[1];
        let (transaction_vec, client_accounts) = extract_records(file_path)?;
        let client_accounts = process_transactions(transaction_vec, client_accounts);
        print_client_accounts_state(client_accounts);
    }

    Ok(())
}

fn extract_records(file_path: &str) -> Result<TransactionsWithAccounts, Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(file_path)?;

    let mut transaction_vec = Vec::new();
    let mut account_map = HashMap::new();

    for entry in reader.deserialize() {
        let transaction: Transaction = entry?;

        if let Vacant(entry) = account_map.entry(transaction.client) {
            entry.insert(ClientAccount::new(transaction.client));
        }

        transaction_vec.push(transaction);
    }

    Ok((transaction_vec, account_map))
}

fn process_transactions(
    transactions: Vec<Transaction>,
    mut accounts: HashMap<ClientId, ClientAccount>,
) -> Vec<ClientState> {
    for transaction in transactions {
        if let Occupied(mut account) = accounts.entry(transaction.client) {
            account.get_mut().apply_transaction(transaction)
        }
    }

    accounts.into_values().map(ClientState::from).collect()
}

fn print_client_accounts_state(accounts: Vec<ClientState>) {
    println!("client,available,held,total,locked");
    for account in accounts {
        println!("{}", account);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use float_cmp;

    impl PartialEq for ClientState {
        fn eq(&self, other: &Self) -> bool {
            self.client == other.client
                && float_cmp::approx_eq!(f32, self.available, other.available, epsilon = 0.00001)
                && float_cmp::approx_eq!(f32, self.held, other.held, epsilon = 0.00001)
                && self.locked == other.locked
        }
    }

    impl Eq for ClientState {}

    impl PartialEq for Transaction {
        fn eq(&self, other: &Self) -> bool {
            self.r#type == other.r#type
                && self.client == other.client
                && self.tx == other.tx
                && float_cmp::approx_eq!(f32, self.amount, other.amount, epsilon = 0.00001)
        }
    }

    impl Eq for Transaction {}

    #[test]
    fn happy_path() {
        let file_path = "test_data/20.csv";

        let expected_result = ClientState {
            client: 1,
            available: 55.0,
            held: 0.0,
            locked: false,
        };

        let (transaction_vec, client_accounts) =
            extract_records(file_path).expect("Should have extracted records");
        assert_eq!(transaction_vec.len(), 20);
        assert_eq!(client_accounts.len(), 1);

        let client_accounts = process_transactions(transaction_vec, client_accounts);
        assert_eq!(client_accounts.len(), 1);
        assert_eq!(client_accounts[0], expected_result);
    }

    #[test]
    fn proper_record_extraction() {
        let file_path = "test_data/sample_types.csv";

        let expected_transactions = vec![
            Transaction {
                r#type: TransactionType::Withdrawal,
                client: 10,
                tx: 119,
                amount: 15.0,
            },
            Transaction {
                r#type: TransactionType::Deposit,
                client: 13,
                tx: 131,
                amount: 15.3,
            },
            Transaction {
                r#type: TransactionType::Dispute,
                client: 20,
                tx: 341,
                amount: 15.5761,
            },
            Transaction {
                r#type: TransactionType::Resolve,
                client: 15,
                tx: 391,
                amount: 415.0,
            },
            Transaction {
                r#type: TransactionType::Chargeback,
                client: 11,
                tx: 319,
                amount: 0.0,
            },
            Transaction {
                r#type: TransactionType::Unknown,
                client: 41,
                tx: 531,
                amount: 165.0,
            },
        ];

        let (transaction_vec, client_accounts) =
            extract_records(file_path).expect("Should have extracted records");

        assert_eq!(transaction_vec.len(), 6);
        assert_eq!(client_accounts.len(), 6);

        for i in 0..6 {
            assert_eq!(transaction_vec[i], expected_transactions[i]);
        }
    }
}
