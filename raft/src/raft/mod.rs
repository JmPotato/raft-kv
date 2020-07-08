use std::collections::HashSet;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use futures::channel::mpsc::UnboundedSender;
use rand::{thread_rng, Rng};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

#[derive(Debug)]
pub enum RPCReply {
    NULL,
    RequestVoteReply(RequestVoteReply),
    AppendEntriesReply(AppendEntriesReply),
}

impl RPCReply {
    pub fn term(&self) -> u64 {
        match self {
            RPCReply::RequestVoteReply(content) => content.term,
            RPCReply::AppendEntriesReply(content) => content.term,
            _ => 0,
        }
    }
}
#[derive(Default, Clone, Debug)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    term: u64,
    // The role of the node
    role: Role,
    // Voted for which node
    voted_for: Option<u64>,
    vote_from: HashSet<u64>,
    // The very first beginning of a raft server's initialization time
    init_time: Instant,
    // Some key time points
    election_start_time: u128,
    election_timeout_time: u128,
    next_heartbeat: Vec<u128>,

    // RPC reply channel
    rpc_reply_tx: Option<Sender<RPCReply>>,

    // Log entries
    log_entries: Vec<Entry>,
    commit_index: u64,
    last_applied: u64,
    next_index: Vec<u64>,
    match_index: Vec<u64>,

    // Used to adjust the append_entries behaviour
    sent_log_length: Vec<usize>,
    sent_failed_count: Vec<u64>,

    /// Apply channel for test
    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            term: 0,
            role: Role::Follower,
            voted_for: None,
            vote_from: HashSet::new(),
            init_time: Instant::now(),
            election_start_time: 0,
            election_timeout_time: 0,
            next_heartbeat: Vec::new(),
            rpc_reply_tx: None,
            log_entries: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            sent_log_length: Vec::new(),
            sent_failed_count: Vec::new(),
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf.become_follower();

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(&self, server: usize, args: RequestVoteArgs) {
        let tx = self.rpc_reply_tx.clone();
        let peer = &self.peers[server as usize];
        let peer_clone = peer.clone();

        if let Some(tx) = tx {
            peer.spawn(async move {
                let res = peer_clone
                    .request_vote(&args)
                    .await
                    .map_or(RPCReply::NULL, RPCReply::RequestVoteReply);
                let _ = tx.send(res);
            });
        }
    }

    fn send_append_entries(&self, server: usize, args: AppendEntriesArgs) {
        info!(
            "Node/Raft #{} as a {:?} with term {} sent a heartbeat to #{}",
            self.me, self.role, self.term, server
        );
        let tx = self.rpc_reply_tx.clone();
        let peer = &self.peers[server as usize];
        let peer_clone = peer.clone();

        if let Some(tx) = tx {
            peer.spawn(async move {
                let res = peer_clone
                    .append_entries(&args)
                    .await
                    .map_or(RPCReply::NULL, RPCReply::AppendEntriesReply);
                let _ = tx.send(res);
            });
        }
    }

    fn send_heartbeat(&mut self, peer: u64) {
        let current_time = self.time_elapsed();
        let next_heartbeat = &mut self.next_heartbeat[peer as usize];
        let send_heartbeat = if current_time - *next_heartbeat >= 100 {
            *next_heartbeat = current_time;
            true
        } else {
            false
        };

        if send_heartbeat {
            let next_index = self.next_index[peer as usize];
            trace!(
                "[Leader] Node #{}, peer #{} last_log_index: {} next_index: {}",
                peer,
                self.me,
                self.last_log_index(),
                next_index
            );
            if self.last_log_index() < next_index {
                // Normal heartbeat
                trace!("[Leader] Node #{} sent normal heartbeat", self.me);
                self.send_append_entries(
                    peer as usize,
                    AppendEntriesArgs {
                        term: self.term,
                        leader_id: self.me as u64,
                        prev_log_index: self.last_log_index(),
                        prev_log_term: self.last_log_term(),
                        leader_commit: self.commit_index,
                        entries: Vec::new(),
                    },
                );
                self.sent_failed_count[peer as usize] = 0;
                self.sent_log_length[peer as usize] = 0;
                return;
            }
            trace!(
                "[Leader] Node #{} has logs: {:?}",
                self.me,
                self.log_entries
            );
            // Send logs with heartbeat
            let logs = self.log_entries.clone();
            let prev_log_index = next_index - 1;
            let prev_log_term = if prev_log_index != 0 {
                logs[prev_log_index as usize - 1].term
            } else {
                0
            };
            // Logs sent will stay in [next_index, end_index]
            let end_index = logs.len();
            let logs_iter = logs[next_index as usize - 1..end_index].iter();
            let mut index_count = 0;
            let entries: Vec<Log> = logs_iter
                .map(|x| {
                    let new_entry = Log {
                        term: self.term,
                        index: next_index - 1 + index_count,
                        command: x.command.clone(),
                    };
                    index_count += 1;
                    new_entry
                })
                .collect();
            trace!(
                "[Leader] Node #{} sent logs: {:?}",
                self.me,
                entries.clone()
            );
            self.send_append_entries(
                peer as usize,
                AppendEntriesArgs {
                    term: self.term,
                    leader_id: self.me as u64,
                    prev_log_index,
                    prev_log_term,
                    leader_commit: self.commit_index,
                    entries,
                },
            );
            self.sent_failed_count[peer as usize] += 1;
            self.sent_log_length[peer as usize] = end_index - next_index as usize + 1;
        }
    }

    fn send_heartbeats(&mut self) {
        for peer in 0..self.peers.len() {
            if peer != self.me {
                self.send_heartbeat(peer as u64);
            }
        }
    }

    // Return the time elapsed from the starting
    fn time_elapsed(&self) -> u128 {
        self.init_time.elapsed().as_millis()
    }

    fn gen_rand_range(&self, low: i128, high: i128) -> u128 {
        let mut rng = thread_rng();
        rng.gen_range(low, high) as u128
    }

    fn become_follower(&mut self) {
        info!(
            "Node/Raft #{} became a follower with term {}",
            self.me, self.term
        );
        self.role = Role::Follower;
        self.voted_for = None;
        self.vote_from.clear();
        self.election_start_time = self.time_elapsed() + self.gen_rand_range(150, 300);
    }

    fn become_candidate(&mut self) {
        // Start election
        self.term += 1;
        self.role = Role::Candidate;
        info!(
            "Node/Raft #{} became a candidate with term {}",
            self.me, self.term
        );
        self.election_timeout_time = self.time_elapsed() + self.gen_rand_range(150, 300);
        self.voted_for = Some(self.me as u64);
        self.vote_from.clear();
        self.vote_from.insert(self.me as u64);
        self.election_timeout_time = self.time_elapsed() + self.gen_rand_range(150, 300);
        for peer in 0..self.peers.len() {
            let peer = peer;
            if peer != self.me {
                self.send_request_vote(
                    peer,
                    RequestVoteArgs {
                        term: self.term,
                        candidate_id: self.me as u64,
                        last_log_index: self.last_log_index(),
                        last_log_term: self.last_log_term(),
                    },
                );
            }
        }
    }

    fn become_leader(&mut self) {
        info!(
            "Node/Raft #{} became a leader with term {}",
            self.me, self.term
        );
        self.role = Role::Leader;
        // Clear volatile states
        self.next_heartbeat = Vec::new();
        self.match_index = Vec::new();
        self.next_index = Vec::new();
        self.sent_failed_count = Vec::new();
        self.sent_log_length = Vec::new();
        for _ in 0..self.peers.len() {
            self.next_heartbeat.push(self.time_elapsed());
            self.match_index.push(0);
            self.next_index.push(self.last_log_index() + 1);
            self.sent_failed_count.push(0);
            self.sent_log_length.push(0);
        }
        self.send_heartbeats();
    }

    // Refresh the raft state
    fn refresh(&mut self) {
        let current_time = self.time_elapsed();
        match self.role {
            Role::Follower => {
                // No leader now, start an election
                if current_time > self.election_start_time {
                    self.become_candidate();
                }
            }
            Role::Candidate => {
                // Not get enough votes, restart a new election round
                if current_time > self.election_timeout_time {
                    self.become_candidate();
                }
            }
            Role::Leader => {
                self.send_heartbeats();
            }
        }
    }

    // check_term() does basic term check to make lower term server become follower
    fn check_term(&mut self, term_received: u64) {
        if self.term < term_received {
            info!(
                "Node/Raft #{} as a {:?} found a higher term {} than {}",
                self.me, self.role, term_received, self.term
            );
            self.term = term_received;
            self.become_follower();
        }
    }

    fn apply_message(&mut self) {
        for idx in self.last_applied + 1..=self.commit_index {
            self.apply_ch
                .unbounded_send(ApplyMsg {
                    command_valid: true,
                    command_index: idx,
                    command: self.log_entries[idx as usize - 1].command.clone(),
                })
                .unwrap();
        }
        self.last_applied = self.commit_index;
    }

    // commit() will commit the latest log if it get majority's aggrement
    fn commit(&mut self) {
        let mut matched_index = self.match_index.clone();
        matched_index[self.me as usize] = self.last_log_index();
        matched_index.sort();
        let committed_index = matched_index[self.peers.len() / 2];
        trace!(
            "Node/Raft #{} as a {:?}, now matched_index: {:?}, committed_index {:?}",
            self.me,
            self.role,
            self.match_index,
            committed_index,
        );
        if committed_index > 0
            && committed_index > self.commit_index
            && self.log_term_of(committed_index as usize) == self.term
        {
            self.commit_index = committed_index;
            self.apply_message();
        }
    }

    pub fn handle_reply(&mut self, reply: RPCReply) {
        self.check_term(reply.term());
        match reply {
            RPCReply::RequestVoteReply(content) => {
                // info!(
                //     "Node/Raft #{} as a {:?} received a RequestVoteReply from #{} said {}",
                //     self.me, self.role, content.from, content.vote_granted
                // );
                if self.role == Role::Candidate && content.vote_granted {
                    self.vote_from.insert(content.from);
                    info!(
                        "Node/Raft #{} as a {:?} received totally {} votes",
                        self.me,
                        self.role,
                        self.vote_from.len()
                    );
                    if self.vote_from.len() * 2 >= self.peers.len() {
                        self.become_leader();
                    }
                }
            }
            RPCReply::AppendEntriesReply(content) => {
                info!(
                    "Node/Raft #{} as a {:?} received a AppendEntriesReply from #{} said {}",
                    self.me, self.role, content.from, content.success
                );
                if self.role == Role::Leader {
                    if content.success {
                        let sent_length = self.sent_log_length[content.from as usize] as u64;
                        if sent_length != 0 {
                            self.match_index[content.from as usize] += sent_length;
                            self.next_index[content.from as usize] += sent_length;
                        }
                        trace!(
                            "Node/Raft #{} as a {:?} received a AppendEntriesReply from #{} sent_log_length: {}, match_index: {}, next_index: {}",
                            self.me, self.role, content.from, sent_length, self.match_index[content.from as usize], self.next_index[content.from as usize]
                        );
                        self.commit();
                    } else {
                        let subtract_size =
                            2_u64.pow(self.sent_failed_count[content.from as usize] as u32) - 1;
                        let prev_match_index =
                            if self.match_index[content.from as usize] >= subtract_size {
                                self.match_index[content.from as usize] - subtract_size
                            } else {
                                0
                            };
                        self.next_index[content.from as usize] = prev_match_index.max(1);
                        self.send_heartbeat(content.from);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn handle_request_vote(
        &mut self,
        args: RequestVoteArgs,
    ) -> labrpc::Result<RequestVoteReply> {
        self.check_term(args.term);
        match self.role {
            Role::Follower => {
                let vote_granted = match self.voted_for {
                    None => {
                        args.last_log_term > self.last_log_term()
                            || ((args.last_log_term == self.last_log_term())
                                && (args.last_log_index >= self.last_log_index()))
                    }
                    Some(candidate_id) => candidate_id == args.candidate_id,
                };
                if vote_granted {
                    self.voted_for = Some(args.candidate_id);
                }
                self.election_start_time = self.time_elapsed() + self.gen_rand_range(150, 300);
                Ok(RequestVoteReply {
                    term: self.term,
                    vote_granted,
                    from: self.me as u64,
                })
            }
            _ => Ok(RequestVoteReply {
                term: self.term,
                vote_granted: false,
                from: self.me as u64,
            }),
        }
    }

    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> labrpc::Result<AppendEntriesReply> {
        self.check_term(args.term);
        if self.term > args.term {
            return Ok(AppendEntriesReply {
                term: self.term,
                success: false,
                from: self.me as u64,
            });
        }
        // Receiving a heatbeat means a leader already existed
        if self.role == Role::Candidate {
            self.become_follower();
        }
        match self.role {
            Role::Follower => {
                self.election_start_time = self.time_elapsed() + self.gen_rand_range(150, 300);
                let mut success = false;
                trace!("[Before] Node #{} logs: {:?}", self.me, self.log_entries);
                trace!(
                    "Node #{} self.prev_log_index: {}, self.prev_log_term: {}, args.prev_log_term: {}",
                    self.me,
                    self.last_log_index(),
                    self.log_term_of(args.prev_log_index as usize),
                    args.prev_log_term,
                );
                if self.log_term_of(args.prev_log_index as usize) == args.prev_log_term {
                    for (index, entry) in args.entries.into_iter().enumerate() {
                        let cur_index = args.prev_log_index as usize + index;
                        if cur_index < self.log_entries.len() {
                            if self.log_term_of(cur_index) != entry.term {
                                self.log_entries.drain(cur_index..);
                                self.log_entries.push(Entry {
                                    term: entry.term,
                                    index: entry.index,
                                    command: entry.command,
                                });
                            } else {
                                self.log_entries[cur_index] = Entry {
                                    term: entry.term,
                                    index: entry.index,
                                    command: entry.command,
                                };
                            }
                        } else {
                            self.log_entries.push(Entry {
                                term: entry.term,
                                index: entry.index,
                                command: entry.command,
                            });
                        }
                    }
                    success = true;
                    info!(
                        "Node/Raft #{} as a {:?} update log from #{} successfully",
                        self.me, self.role, args.leader_id
                    );
                }
                trace!(
                    "Node #{} args.leader_commit: {}, self.commit_index: {}",
                    self.me,
                    args.leader_commit,
                    self.commit_index
                );
                if args.leader_commit > self.commit_index {
                    self.commit_index = self.last_log_index().min(args.leader_commit);
                    self.apply_message();
                }
                trace!("[After] Node #{} logs: {:?}", self.me, self.log_entries);
                Ok(AppendEntriesReply {
                    term: self.term,
                    success,
                    from: self.me as u64,
                })
            }
            _ => Ok(AppendEntriesReply {
                term: self.term,
                success: false,
                from: self.me as u64,
            }),
        }
    }

    fn append_log(&mut self, command: Vec<u8>) -> u64 {
        self.log_entries.push(Entry {
            index: self.last_log_index() + 1,
            term: self.term,
            command,
        });

        self.last_log_index()
    }

    fn last_log_index(&self) -> u64 {
        self.log_entries.len() as u64
    }

    fn last_log_term(&self) -> u64 {
        self.log_term_of(self.last_log_index() as usize)
    }

    fn log_term_of(&self, index: usize) -> u64 {
        if index == 0 {
            0
        } else {
            self.log_entries[index - 1].term
        }
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role == Role::Leader {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            let index = self.append_log(buf);
            Ok((index, self.term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        self.persist();
        let _ = &self.persister;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Option<Raft>>>,
    // Tik-tok thread
    timer: Arc<Option<JoinHandle<()>>>,
    // RPC reply listener thread
    listener: Arc<Option<JoinHandle<()>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        let (tx, rx) = channel();
        raft.rpc_reply_tx = Some(tx);
        let me = raft.me;

        let mut node = Node {
            raft: Arc::new(Mutex::new(Some(raft))),
            timer: Arc::new(None),
            listener: Arc::new(None),
        };

        // timer thread is used to be make a raft node refresh its state periodically
        let raft = node.raft.clone();
        node.timer = Arc::new(Some(thread::spawn(move || {
            info!("Node/Raft #{} started the timer", me);
            loop {
                let mut raft = raft.lock().unwrap();
                if let Some(raft) = raft.as_mut() {
                    raft.refresh();
                } else {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }
            info!("Node/Raft #{} stopped the timer", me);
        })));

        // listener thread is used to handle the rpc reply from another peer
        let raft = node.raft.clone();
        node.listener = Arc::new(Some(thread::spawn(move || {
            info!("Node/Raft #{} started the rpc listener", me);
            for reply in rx.iter() {
                let mut raft = raft.lock().unwrap();
                if let Some(raft) = raft.as_mut() {
                    raft.handle_reply(reply);
                }
                std::thread::yield_now();
            }
            info!("Node/Raft #{} stopped the rpc listener", me);
        })));

        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if let Some(raft) = self.raft.lock().unwrap().as_mut() {
            raft.start(command)
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let mut raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            State {
                is_leader: raft.role == Role::Leader,
                term: raft.term,
            }
        } else {
            State {
                is_leader: false,
                term: 0,
            }
        }
    }

    pub fn me(&self) -> usize {
        let mut raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            return raft.me;
        }
        3
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        let mut raft = self.raft.lock().unwrap();
        *raft = None;
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let raft = self.raft.clone();
        let mut raft = raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            raft.handle_request_vote(args)
        } else {
            Err(labrpc::Error::Stopped)
        }
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let raft = self.raft.clone();
        let mut raft = raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            raft.handle_append_entries(args)
        } else {
            Err(labrpc::Error::Stopped)
        }
    }
}
