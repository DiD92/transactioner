use std::collections::{hash_map::Entry::Occupied, hash_map::Entry::Vacant, HashMap, HashSet};
use std::{env, fs};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::path::Path;

use serde::{Deserialize, Deserializer};
use tokio::runtime::Builder;
use twox_hash::RandomXxHashBuilder64;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;

type ClientId = u16;
type ClientAccounts = HashMap<ClientId, ClientAccount, RandomXxHashBuilder64>;

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

#[derive(Debug, Deserialize, Copy, Clone)]
struct Transaction {
    #[serde(deserialize_with = "transaction_type_deserializer")]
    r#type: TransactionType,
    client: ClientId,
    tx: u32,
    amount: f32,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Copy, Clone)]
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
    transactions: HashMap<u32, f32, RandomXxHashBuilder64>,
    disputed_transactions: HashSet<u32, RandomXxHashBuilder64>,
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
                if let Vacant(entry) = self.transactions.entry(transaction.tx) {
                    self.available += transaction.amount;
                    entry.insert(transaction.amount);
                }
            }
            TransactionType::Withdrawal if self.available >= transaction.amount => {
                if let Vacant(entry) = self.transactions.entry(transaction.tx) {
                    self.available -= transaction.amount;
                    entry.insert(-transaction.amount);
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
            _ => {
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
        let file_path = Path::new(&args[1]).to_owned();
        let metadata = fs::metadata(&file_path)?;

        if !file_path.exists() {
            eprintln!("File path is invalid, exiting...");

            return Ok(())
        }

        // After some profiling, it seems that the general best amount of worker is only 2, the limiting factor in the
        // code seems to be the speed at which you can read the CSV file, so more threads aren't worth it unless
        // significant increases in read performance are achieved.
        let num_workers = 2;
        // Here we try to estimate the best buffer size taking into account the amount of work each worker is going to process
        // the more work each worker has assigned the higher the chance a small buffer may be filled before being processed
        let work_per_worker = ((metadata.len() as usize / num_workers) / 25_000_000) + 1;
        // Min buffer size is 120KB max size is 60MB
        let buffer_size = std::cmp::min(10_000 * work_per_worker, 5_000_000);

        eprintln!("Using {} worker thread/s to process {:?} using a channel buffer size of {} Bytes", num_workers, &file_path, buffer_size * std::mem::size_of::<Transaction>());

        let rt = Builder::new_multi_thread()
            .worker_threads(num_workers + 1)
            .build()?;

        rt.block_on(async {
            let mut handle_set = Vec::with_capacity(num_workers);
            let mut sender_set = Vec::with_capacity(num_workers);
            let results_vec = Arc::new(Mutex::new(Vec::with_capacity(num_workers)));

            for _ in 0..num_workers {
                let (tx, mut rx) = tokio::sync::mpsc::channel(buffer_size);
                sender_set.push(tx);
                let worker_results_vec = results_vec.clone();
                handle_set.push(rt.spawn(async move {
                    let mut account_map = ClientAccounts::default();
                    while let Some(transaction) = rx.recv().await {
                        process_transaction(transaction, &mut account_map)
                    }

                    if let Ok(mut data) = worker_results_vec.lock() {
                        data.push(account_map.into_values().map(ClientState::from).collect());
                    }
                }));
            }

            handle_set.push(rt.spawn(async move {
                let _ = extract_records(file_path, num_workers, sender_set).await;
            }));

            futures::future::join_all(handle_set).await;

            if let Ok(data) = results_vec.lock() {
                print_client_accounts_state(data.as_ref());
            };
        });
    }

    Ok(())
}

async fn extract_records<P: AsRef<Path>>(
    file_path: P,
    num_workers: usize,
    sender_vec: Vec<Sender<Transaction>>
) -> Result<(), Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(file_path)?;

    for entry in reader.deserialize() {
        let transaction: Transaction = entry?;

        let worker_index = transaction.client as usize % num_workers;

        if let Err(e) = sender_vec[worker_index].try_send(transaction) {
            if let TrySendError::Full(_) = e {
                eprintln!("Buffer full for worker {}, waiting...", worker_index);
                sender_vec[worker_index].send(transaction).await?;
            } else {
                return Err(Box::new(e));
            }
        }
    }

    Ok(())
}

fn process_transaction(tx: Transaction, accounts: &mut ClientAccounts) {
    match accounts.entry(tx.client) {
        Occupied(mut account) => account.get_mut().apply_transaction(tx),
        Vacant(entry) => {
            let mut account = ClientAccount::new(tx.client);
            account.apply_transaction(tx);
            entry.insert(account);
        }
    }
}

fn print_client_accounts_state(accounts: &[Vec<ClientState>]) {
    println!("client,available,held,total,locked");
    for account_group in accounts {
        for account in account_group {
            println!("{}", account);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use float_cmp::approx_eq;

    const EPSILON: f32 = 0.00001;

    impl PartialEq for ClientState {
        fn eq(&self, other: &Self) -> bool {
            self.client == other.client
                && approx_eq!(f32, self.available, other.available, epsilon = EPSILON)
                && approx_eq!(f32, self.held, other.held, epsilon = EPSILON)
                && self.locked == other.locked
        }
    }

    impl Eq for ClientState {}

    impl PartialEq for Transaction {
        fn eq(&self, other: &Self) -> bool {
            self.r#type == other.r#type
                && self.client == other.client
                && self.tx == other.tx
                && approx_eq!(f32, self.amount, other.amount, epsilon = EPSILON)
        }
    }

    impl Eq for Transaction {}

    #[tokio::test]
    async fn happy_path() {
        let file_path = "test_data/20.csv";

        let expected_result = ClientState {
            client: 1,
            available: 55.0,
            held: 0.0,
            locked: false,
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            extract_records(file_path, 1, vec![tx]).await.expect("Should finish correctly");
        });

        let mut transaction_vec = Vec::with_capacity(20);

        while let Some(data) = rx.recv().await {
            transaction_vec.push(data);
        }

        assert_eq!(transaction_vec.len(), 20);

        let mut accounts = ClientAccounts::default();

        for transactions in transaction_vec {
            process_transaction(transactions, &mut accounts)
        }

        let account_states: Vec<ClientState> = accounts.into_values().map(ClientState::from).collect();

        assert_eq!(account_states.len(), 1);
        assert_eq!(account_states[0], expected_result);
    }

    #[tokio::test]
    async fn happy_path_with_all_types() {
        let file_path = "test_data/15.csv";

        let expected_results = vec![
            ClientState {
                client: 1,
                available: 100.0,
                held: 0.0,
                locked: true,
            },
            ClientState {
                client: 2,
                available: 135.0,
                held: 0.0,
                locked: false,
            },
            ClientState {
                client: 3,
                available: 100.0,
                held: 0.0,
                locked: false,
            },
        ];

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            extract_records(file_path, 1, vec![tx]).await.expect("Should finish correctly");
        });

        let mut transaction_vec = Vec::with_capacity(20);

        while let Some(data) = rx.recv().await {
            transaction_vec.push(data);
        }

        assert_eq!(transaction_vec.len(), 15);

        let mut accounts = ClientAccounts::default();

        for transactions in transaction_vec {
            process_transaction(transactions, &mut accounts)
        }

        let mut account_states: Vec<ClientState> = accounts.into_values().map(ClientState::from).collect();
        account_states.sort_by_key(|x| x.client);

        assert_eq!(account_states.len(), 3);
        for i in 0..3 {
            assert_eq!(account_states[i], expected_results[i]);
        }


    }

    #[tokio::test]
    async fn proper_record_extraction() {
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

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            extract_records(file_path, 1, vec![tx]).await.expect("Should finish correctly");
        });

        let mut transaction_vec = Vec::with_capacity(20);

        while let Some(data) = rx.recv().await {
            transaction_vec.push(data);
        }

        assert_eq!(transaction_vec.len(), 6);
        for i in 0..6 {
            assert_eq!(transaction_vec[i], expected_transactions[i]);
        }
    }
}
