use byteorder::ByteOrder;
use procfs::process::Process;
use rand::Rng;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static MORE_TASKS_COUNT: usize = 20;
static MIN_GRAPH_SIZE: usize = 50 * 1024 * 1024;
static MAX_GRAPH_SIZE: usize = 100 * 1024 * 1024;
static MIN_RULE_SIZE: usize = 50 * 1024;
static MAX_RULE_SIZE: usize = 20 * 1024 * 1024;
static THREAD_COUNT: usize = 6;

/// This application is designed to debug memory usage in sled under a real-world workload. That workload consists of
/// several usage patterns:
/// 1. Very large graphs inserted every few minutes and read frequently until the next graph is inserted. Never deleted
/// 2. Very tiny tasks repeatedly pushed and popped at a high frequency
/// 3. Medium sized rules inserted semi-randomly and read frequently. Never deleted
fn main() {
    // Open the sled database file
    let db = Arc::new(sled::open("test_db").unwrap());

    let mut use_cas = false;
    for argument in env::args() {
        if argument == "--use-cas" {
            println!("Using CAS instead of plain insert");
            use_cas = true;
        }
    }

    // Setup the atomic IDs
    let current_graph_id = Arc::new(AtomicU64::new(0));
    let max_rule_id = Arc::new(AtomicU64::new(0));

    // If there is no graph, create the first one
    let graph_tree = db.open_tree("graphs").unwrap();
    if graph_tree.first().unwrap().is_none() {
        let graph = vec![0u8; MIN_GRAPH_SIZE];
        let graph_id = db.generate_id().unwrap();
        graph_tree.insert(u64_bytes(graph_id), &graph[..]).unwrap();
        current_graph_id.store(graph_id, Ordering::Relaxed);
    }

    // If there are no rules, create the first one
    let rule_tree = db.open_tree("rules").unwrap();
    if rule_tree.first().unwrap().is_none() {
        let rule = vec![0u8; MIN_RULE_SIZE];
        let rule_id = max_rule_id.fetch_add(1, Ordering::SeqCst);
        rule_tree.insert(u64_bytes(rule_id), &rule[..]).unwrap();
    }

    for _ in 0..THREAD_COUNT {
        let t_db = db.clone();
        let t_cgi = current_graph_id.clone();
        let t_mri = max_rule_id.clone();
        thread::spawn(move || {
            run_tasks(&t_db, &t_cgi, &t_mri, use_cas);
        });
    }

    // Print out memory usage every 5 seconds
    let page_size = procfs::page_size().unwrap() as u64;
    let start = Instant::now();
    println!(" Time |    Virtual    |    Resident   |");
    loop {
        let elapsed = Instant::now().duration_since(start).as_secs();
        let me = Process::myself().unwrap();
        println!(" {:>4} | {:>13} | {:>13} |", elapsed, me.stat.vsize, me.stat.rss as u64 * page_size);
        thread::sleep(Duration::from_millis(5000));
    }
}

fn u64_bytes(value: u64) -> [u8; 8] {
    let mut buf = [0u8; 8];
    byteorder::BE::write_u64(&mut buf, value);
    buf
}

fn insert_task(db: &sled::Db, tree: &sled::Tree, task: TaskType) {
    let task_id = db.generate_id().unwrap();
    tree.insert(u64_bytes(task_id), &[task as u8; 1]).unwrap();
}

fn run_tasks(db: &sled::Db, current_graph_id: &AtomicU64, max_rule_id: &AtomicU64, use_cas: bool) {
    let mut rng = rand::thread_rng();

    loop {
        let task_tree = db.open_tree("tasks").unwrap();
        let next_task = task_tree.pop_min().unwrap();
        if let Some((_key, task)) = next_task {
            match TaskType::from_u8(task[0]) {
                TaskType::NewGraph => {
                    // Create a randomly sized graph
                    let size = rng.gen_range(MIN_GRAPH_SIZE..MAX_GRAPH_SIZE);
                    let graph = vec![0u8; size];
                    let graph_id = db.generate_id().unwrap();
                    let graph_tree = db.open_tree("graphs").unwrap();

                    // In CAS mode we implement 'insert if not present'
                    if use_cas {
                        graph_tree.compare_and_swap(u64_bytes(graph_id), None as Option<&[u8]>, Some(&graph[..])).unwrap().unwrap();
                    } else {
                        // No CAS, we just insert
                        graph_tree.insert(u64_bytes(graph_id), &graph[..]).unwrap();
                    }

                    // Tell the other threads the new 'current' graph. It doesn't really matter if they load an
                    // outdated graph_id
                    current_graph_id.store(graph_id, Ordering::Relaxed);
                }
                TaskType::NewRule => {
                    // Create a randomly sized rule
                    let size = rng.gen_range(MIN_RULE_SIZE..MAX_RULE_SIZE);
                    let rule = vec![0u8; size];
                    let rule_id = max_rule_id.fetch_add(1, Ordering::SeqCst);
                    let rule_tree = db.open_tree("rules").unwrap();
                    rule_tree.insert(u64_bytes(rule_id), &rule[..]).unwrap();
                }
                TaskType::RuleAndGraphCalculation => {
                    // Read the current graph
                    let graph_id = current_graph_id.load(Ordering::Relaxed);
                    let graph_tree = db.open_tree("graphs").unwrap();
                    graph_tree.get(u64_bytes(graph_id)).unwrap();

                    // Read all the rules
                    let rule_tree = db.open_tree("rules").unwrap();
                    for _item in rule_tree.iter() {}
                }
                TaskType::RuleCalculation => {
                    // Pick a random rule to read
                    let rule_id = rng.gen_range(0..max_rule_id.load(Ordering::SeqCst)) + 1;
                    let rule_tree = db.open_tree("rules").unwrap();
                    rule_tree.get(u64_bytes(rule_id)).unwrap();
                }
                TaskType::MoreTasks => {
                    // Insert another chunk of randomly selected tasks
                    for _i in 0..MORE_TASKS_COUNT {
                        let index = rng.gen_range(0..TASK_FREQUENCY.len());
                        insert_task(&db, &task_tree, TASK_FREQUENCY[index]);
                    }
                    // Insert a task to create more tasks
                    insert_task(&db, &task_tree, TaskType::MoreTasks);
                }
            }
        } else {
            // No tasks in queue, insert a MoreTasks task
            insert_task(&db, &task_tree, TaskType::MoreTasks);
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
enum TaskType {
    NewGraph,
    NewRule,
    RuleCalculation,
    RuleAndGraphCalculation,
    MoreTasks,
}

/// This array represents the relative frequency of operations
static TASK_FREQUENCY: [TaskType; 66] = [
    // NewGraph is the least frequent
    TaskType::NewGraph,

    // NewRule happens about 5x more frequently than NewGraph
    TaskType::NewRule,
    TaskType::NewRule,
    TaskType::NewRule,
    TaskType::NewRule,
    TaskType::NewRule,

    // Calculations reading all the rules and the current graph occur about 3x for every new rule
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,
    TaskType::RuleAndGraphCalculation,

    // Calculations that read a single rule form the bulk of the operations
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
    TaskType::RuleCalculation,
];

impl TaskType {
    fn from_u8(value: u8) -> TaskType {
        // Match doesn't work
        if value == TaskType::NewGraph as u8 {
            TaskType::NewGraph
        } else if value == TaskType::NewRule as u8 {
            TaskType::NewRule
        } else if value == TaskType::RuleCalculation as u8 {
            TaskType::RuleCalculation
        } else if value == TaskType::RuleAndGraphCalculation as u8 {
            TaskType::RuleAndGraphCalculation
        } else {
            TaskType::MoreTasks
        }
    }
}