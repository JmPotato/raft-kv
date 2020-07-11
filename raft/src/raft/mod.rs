use futures::channel::mpsc::UnboundedSender;
use fxhash::{FxHashMap, FxHashSet};
use rand::{thread_rng, Rng};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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

// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    // The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    // Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(PartialEq, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub enum RPCReply {
    RequestVoteReply(RequestVoteReply),
    AppendEntriesReply(AppendEntriesReply),
}

impl RPCReply {
    pub fn term(&self) -> u64 {
        match self {
            RPCReply::RequestVoteReply(x) => x.term,
            RPCReply::AppendEntriesReply(x) => x.term,
        }
    }
}

const MAX_ENTRY: usize = 100;

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // This peer's index into peers[]
    me: u64,

    // More details see: https://raft.github.io/raft.pdf
    // The role of Raft instance
    role: Role,
    // Some persistent states(W.I.P)
    // Current term
    term: u64,
    // Candidate's ID that received vote in current term (or null if none)
    voted_for: Option<u64>,
    // Should be reset when become candidate.
    vote_from: FxHashSet<u64>,
    // Each entry contains command for state machine, and term when
    // entry was received by leader and the first index should be 1
    log_entries: Vec<(u64, Vec<u8>)>,

    // Index of highest log entry known to be committed
    // Initialized to 0, increases monotonically
    // Volatile state on all servers
    commit_index: u64,
    // Index of highest log entry applied to state machine
    // Initialized to 0, increases monotonically
    // Volatile state on all servers
    last_applied: u64,

    // For each server, index of the next log entry
    // to send to that server (initialized to leader last log index + 1)
    // Volatile state on leaders
    next_index: Vec<u64>,
    // For each server, index of highest log entry
    // known to be replicated on server
    // Initialized to 0, increases monotonically
    // Volatile state on leaders
    match_index: Vec<u64>,

    // The time point of booting Raft instance
    boot_point: Instant,
    // Follower will start an election at this point
    // Should be reset when become follower.
    election_start_point: u128,
    // Candidate will fail an election at this point
    // Should be reset when become candidate.
    election_timeout_point: u128,
    // The next heartbeat point
    // Volatile state on leaders
    heartbeat_point: Vec<u128>,

    // RPC reply will be sent through this channel
    rpc_reply_channel_tx: Option<Sender<(u64, u64, RPCReply)>>,
    // Used to pair RPC request with RPC response, which is periodically cleared.
    // RPC ID -> (prev_log_index, current_tick, entries_length, failed attempt)
    rpc_append_entries_log_idx: FxHashMap<u64, (u64, u128, u64, u64)>,
    // Used to mark each RPC request
    rpc_id_counter: u64,
    // Used to update RPC cache
    cache_next_update: u128,

    // Apply channel
    apply_ch: UnboundedSender<ApplyMsg>,
    // Apply message on tick in order not to block RPC
    apply_message_flag: bool,
}

impl Raft {
    // the service or tester wants to create a Raft server. the ports
    // of all the Raft servers (including this one) are in peers. this
    // server's port is peers[me]. all the servers' peers arrays
    // have the same order. persister is a place for this server to
    // save its persistent state, and also initially holds the most
    // recent saved state, if any. apply_ch is a channel on which the
    // tester or service expects Raft to send ApplyMsg messages.
    // This method must return quickly.
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
            me: me as u64,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            vote_from: FxHashSet::default(),
            log_entries: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            boot_point: Instant::now(),
            election_start_point: 0,
            election_timeout_point: 0,
            heartbeat_point: Vec::new(),
            rpc_reply_channel_tx: None,
            rpc_append_entries_log_idx: FxHashMap::default(),
            rpc_id_counter: 0,
            cache_next_update: 0,
            apply_ch,
            apply_message_flag: false,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf.become_follower();

        rf
    }

    // Log entries related methods below
    fn logs(&self) -> &Vec<(u64, Vec<u8>)> {
        &self.log_entries
    }

    pub fn logs_mut(&mut self) -> &mut Vec<(u64, Vec<u8>)> {
        &mut self.log_entries
    }

    pub fn debug_logs(&self) {
        if log_enabled!(log::Level::Trace) {
            let mut x = String::new();
            for (term, log) in self.logs().iter() {
                x += format!("{} {} ({:?}), ", term, log.len(), log).as_ref();
            }
            trace!(
                "#{} commit_index={} log_entries={}",
                self.me,
                self.commit_index,
                x
            );
        }
    }

    fn last_log_index(&self) -> u64 {
        self.logs().len() as u64
    }

    fn last_log_term(&self) -> u64 {
        self.log_term_of(self.last_log_index())
    }

    fn log_term_of(&self, index: u64) -> u64 {
        if index == 0 {
            0
        } else {
            self.logs()[index as usize - 1].0
        }
    }

    fn commit(&mut self) {
        let mut latest_match: Vec<u64> = self.match_index.clone();
        latest_match[self.me as usize] = self.last_log_index();
        latest_match.sort();
        let commit_idx = latest_match[self.peers.len() / 2];
        debug!("#{} match index {:?}", self.me, self.match_index);
        if commit_idx > 0
            && commit_idx > self.commit_index
            && self.logs()[commit_idx as usize - 1].0 == self.term
        {
            self.commit_index = commit_idx;
            debug!(
                "#{} leader commit {:?} => {}",
                self.me, self.match_index, self.commit_index
            );
            self.apply_message_flag = true;
        }
    }

    fn apply_message(&mut self) {
        for idx in self.last_applied + 1..=self.commit_index {
            self.apply_ch
                .unbounded_send(ApplyMsg {
                    command_valid: true,
                    command_index: idx,
                    command: self.logs()[idx as usize - 1].1.clone(),
                })
                .unwrap();
        }
        self.last_applied = self.commit_index;
    }

    // Role related methods below
    fn become_follower(&mut self) {
        debug!(
            "#{} role changed: {:?} -> {:?}",
            self.me,
            self.role,
            Role::Follower
        );
        self.role = Role::Follower;
        self.voted_for = None;
        self.election_start_point = self.current_tick() + Self::tick_election_start_at();
    }

    fn become_candidate(&mut self) {
        debug!(
            "#{} role changed: {:?} -> {:?}",
            self.me,
            self.role,
            Role::Candidate
        );
        self.term += 1;
        self.role = Role::Candidate;
        self.start_election();
    }

    fn become_leader(&mut self) {
        debug!(
            "#{} role changed: {:?} -> {:?}",
            self.me,
            self.role,
            Role::Leader
        );
        self.role = Role::Leader;
        self.match_index = Vec::new();
        self.next_index = Vec::new();
        self.heartbeat_point = Vec::new();
        for _ in 0..self.peers.len() {
            self.heartbeat_point.push(self.current_tick());
            self.match_index.push(0);
            self.next_index.push(self.last_log_index() + 1);
        }
        self.heartbeats();
    }

    fn start_election(&mut self) {
        self.voted_for = Some(self.me);
        self.vote_from = {
            let mut hashset = FxHashSet::default();
            hashset.insert(self.me);
            hashset
        };
        self.election_timeout_point = self.current_tick() + Self::tick_election_timeout_at();
        for peer in 0..self.peers.len() {
            let peer = peer as u64;
            if peer != self.me {
                self.send_request_vote(
                    peer,
                    RequestVoteArgs {
                        term: self.term,
                        candidate_id: self.me,
                        last_log_index: self.last_log_index(),
                        last_log_term: self.last_log_term(),
                    },
                );
            }
        }
    }

    // Ticker related methods below
    // Get current time point since the instance started
    fn current_tick(&self) -> u128 {
        self.boot_point.elapsed().as_millis()
    }

    // Generate random election start point with range [150, 300)
    fn tick_election_start_at() -> u128 {
        thread_rng().gen_range(150, 300) as u128
    }

    // Generate random election timeout point with range [150, 300)
    fn tick_election_timeout_at() -> u128 {
        thread_rng().gen_range(150, 300) as u128
    }

    // The next heartbeat time point interval
    fn heartbeat_interval() -> u128 {
        100
    }

    // Update RPC request cache
    fn update_rpc_cache(&mut self) {
        let current_tick = self.current_tick();
        if current_tick > self.cache_next_update {
            self.cache_next_update = current_tick + 1000;
            let expired = std::mem::take(&mut self.rpc_append_entries_log_idx);
            for (req, (idx, timestamp, length, failed_attempt)) in expired.into_iter() {
                if timestamp + 3000 >= current_tick {
                    self.rpc_append_entries_log_idx
                        .insert(req, (idx, timestamp, length, failed_attempt));
                }
            }
        }
    }

    // save Raft's persistent state to stable storage,
    // where it can later be retrieved after a crash and restart.
    // see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    // restore previously persisted state.
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

    // example code to send a RequestVote RPC to a server.
    // server is the index of the target server in peers.
    // expects RPC arguments in args.
    //
    // The labrpc package simulates a lossy network, in which servers
    // may be unreachable, and in which requests and replies may be lost.
    // This method sends a request and waits for a reply. If a reply arrives
    // within a timeout interval, This method returns Ok(_); otherwise
    // this method returns Err(_). Thus this method may not return for a while.
    // An Err(_) return can be caused by a dead server, a live server that
    // can't be reached, a lost request, or a lost reply.
    //
    // This method is guaranteed to return (perhaps after a delay) *except* if
    // the handler function on the server side does not return.  Thus there
    // is no need to implement your own timeouts around this method.
    //
    // look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(&mut self, server: u64, args: RequestVoteArgs) -> u64 {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let rpc_id = self.rpc_id_counter;
        self.rpc_id_counter += 1;
        let rpc_peer = &self.peers[server as usize];
        let rpc_peer_clone = rpc_peer.clone();
        let tx_channel = self.rpc_reply_channel_tx.clone();

        if let Some(tx_channel) = tx_channel {
            rpc_peer.spawn(async move {
                let res = rpc_peer_clone.request_vote(&args).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    tx_channel
                        .send((rpc_id, server, RPCReply::RequestVoteReply(res)))
                        .unwrap();
                }
            });
        }
        rpc_id
    }

    fn send_append_entries(&mut self, server: u64, args: AppendEntriesArgs) -> u64 {
        let rpc_id = self.rpc_id_counter;
        self.rpc_id_counter += 1;
        let rpc_peer = &self.peers[server as usize];
        let rpc_peer_clone = rpc_peer.clone();
        let tx_channel = self.rpc_reply_channel_tx.clone();

        if let Some(tx_channel) = tx_channel {
            rpc_peer.spawn(async move {
                let res = rpc_peer_clone
                    .append_entries(&args)
                    .await
                    .map_err(Error::Rpc);
                if let Ok(res) = res {
                    tx_channel
                        .send((rpc_id, server, RPCReply::AppendEntriesReply(res)))
                        .unwrap();
                }
            });
        }
        rpc_id
    }

    fn heartbeat(&mut self, peer: u64) {
        let current_tick = self.current_tick();
        let heartbeat_point = &mut self.heartbeat_point[peer as usize];
        let send_heartbeat_flag = if current_tick - *heartbeat_point >= Self::heartbeat_interval() {
            *heartbeat_point = current_tick;
            true
        } else {
            false
        };

        if send_heartbeat_flag {
            self.send_append_entries(
                peer,
                AppendEntriesArgs {
                    term: self.term,
                    leader_id: self.me,
                    prev_log_term: self.last_log_term(),
                    prev_log_index: self.last_log_index(),
                    entries: Vec::new(),
                    entries_term: Vec::new(),
                    leader_commit: self.commit_index,
                },
            );
        }
    }

    fn heartbeats(&mut self) {
        for peer in 0..self.peers.len() {
            let peer = peer as u64;
            if peer != self.me {
                self.send_logs_to(peer, 0);
            }
        }
    }

    fn send_logs_to(&mut self, peer: u64, failed_attempt: u64) {
        let next_index = self.next_index[peer as usize];
        if self.last_log_index() < next_index {
            // Normal heartbeat
            self.heartbeat(peer);
            return;
        }
        let prev_log_index = next_index - 1;
        let log = self.logs();
        let end_index = if next_index as usize + MAX_ENTRY <= log.len() {
            next_index as usize + MAX_ENTRY
        } else {
            log.len()
        };
        let entries_iter = log[next_index as usize - 1..end_index].iter();
        let entries_iter_term = entries_iter.clone();

        let entries: Vec<Vec<u8>> = entries_iter.map(|x| x.1.clone()).collect();
        let entries_term: Vec<u64> = entries_iter_term.map(|x| x.0).collect();

        let entries_length = entries.len();

        let rpc_id = self.send_append_entries(
            peer,
            AppendEntriesArgs {
                term: self.term,
                leader_id: self.me,
                prev_log_term: self.log_term_of(next_index - 1),
                prev_log_index,
                leader_commit: self.commit_index,
                entries_term,
                entries,
            },
        );

        self.rpc_append_entries_log_idx.insert(
            rpc_id,
            (
                prev_log_index,
                self.current_tick(),
                entries_length as u64,
                failed_attempt,
            ),
        );
    }

    pub fn handle_request_vote(
        &mut self,
        args: RequestVoteArgs,
    ) -> labrpc::Result<RequestVoteReply> {
        if args.term > self.term {
            self.become_follower();
            self.term = args.term;
        }
        match self.role {
            Role::Follower => {
                let vote_granted = match self.voted_for {
                    Some(candidate_id) => candidate_id == args.candidate_id,
                    None => {
                        args.last_log_term > self.last_log_term()
                            || (args.last_log_term == self.last_log_term()
                                && args.last_log_index >= self.last_log_index())
                    }
                };
                if vote_granted {
                    self.voted_for = Some(args.candidate_id);
                }
                self.election_start_point =
                    self.boot_point.elapsed().as_millis() + Self::tick_election_start_at();
                Ok(RequestVoteReply {
                    term: self.term,
                    vote_granted,
                })
            }
            _ => Ok(RequestVoteReply {
                term: self.term,
                vote_granted: false,
            }),
        }
    }

    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> labrpc::Result<AppendEntriesReply> {
        if args.term > self.term {
            self.term = args.term;
            self.become_follower();
        }
        if args.term < self.term {
            return Ok(AppendEntriesReply {
                term: self.term,
                success: false,
            });
        }
        // Heartbeat means a leader already existed
        if self.role == Role::Candidate && args.term == self.term {
            self.become_follower();
        }
        match self.role {
            Role::Follower => {
                let me = self.me;
                self.election_start_point = self.current_tick() + Self::tick_election_start_at();

                let mut ok = false;
                if args.term < self.term {
                    trace!("#{} append entries failed: got from a lower term", me);
                } else if args.prev_log_index > self.last_log_index() {
                    trace!("#{} append entries failed: missing log(s)", me);
                } else if self.log_term_of(args.prev_log_index) == args.prev_log_term {
                    ok = true;
                } else {
                    trace!(
                        "#{} append entries failed: prev log with index={} is not match with term",
                        me,
                        args.prev_log_index
                    );
                }
                if ok {
                    let log_entries = self.logs_mut();
                    let length = args.entries.len();
                    for (idx, log) in args
                        .entries_term
                        .into_iter()
                        .zip(args.entries.into_iter())
                        .enumerate()
                    {
                        let log_idx = args.prev_log_index as usize + idx;
                        if log_idx < log_entries.len() {
                            if log_entries[log_idx].0 != log.0 {
                                log_entries.drain(log_idx..);
                                log_entries.push(log);
                                trace!("#{} drain log to length {}", me, log_entries.len());
                            } else {
                                log_entries[log_idx] = log;
                            }
                        } else {
                            log_entries.push(log);
                        }
                    }
                    trace!(
                        "#{} append entries successfully with length {}, totally {} now",
                        me,
                        length,
                        log_entries.len()
                    );
                    self.debug_logs();
                    if args.leader_commit > self.commit_index {
                        self.commit_index = self.last_log_index().min(args.leader_commit);
                        self.apply_message_flag = true;
                        debug!("@{} leader commit: {}", me, self.commit_index);
                    }
                }
                Ok(AppendEntriesReply {
                    term: self.term,
                    success: ok,
                })
            }
            _ => Ok(AppendEntriesReply {
                term: self.term,
                success: false,
            }),
        }
    }

    // Handle RPC reply
    pub fn handle_reply(&mut self, rpc_id: u64, from: u64, reply: RPCReply) {
        if reply.term() > self.term {
            self.become_follower();
            self.term = reply.term();
        }
        match self.role {
            Role::Candidate => self.handle_reply_as_candidate(rpc_id, from, reply),
            Role::Leader => self.handle_reply_as_leader(rpc_id, from, reply),
            _ => {}
        }
    }

    pub fn handle_reply_as_candidate(&mut self, _rpc_id: u64, from: u64, reply: RPCReply) {
        if let RPCReply::RequestVoteReply(reply) = reply {
            if reply.vote_granted {
                self.vote_from.insert(from);
                if self.vote_from.len() * 2 >= self.peers.len() {
                    self.become_leader();
                }
            }
        }
    }

    pub fn handle_reply_as_leader(&mut self, rpc_id: u64, from: u64, reply: RPCReply) {
        if let RPCReply::AppendEntriesReply(reply) = reply {
            let prev_match_index = self.rpc_append_entries_log_idx.get(&rpc_id);
            if prev_match_index.is_none() {
                return;
            }
            let (prev_match_index, _, length, failed_attempt) = prev_match_index.unwrap();
            let length = *length;
            let prev_match_index = *prev_match_index;
            let failed_attempt = *failed_attempt;
            self.rpc_append_entries_log_idx.remove(&rpc_id);

            if reply.success {
                self.match_index[from as usize] = prev_match_index + length;
                self.next_index[from as usize] = prev_match_index + length + 1;
                self.commit();
            } else {
                let subtract_size = 2_u64.pow(failed_attempt as u32) - 1;
                let prev_match_index = if prev_match_index > subtract_size {
                    prev_match_index - subtract_size
                } else {
                    0
                };
                self.next_index[from as usize] = prev_match_index.max(1);
                self.send_logs_to(from, failed_attempt + 1);
                debug!(
                    "#{} -> {} append entries failed, prev_match_index={}, attempt={}",
                    self.me, from, prev_match_index, failed_attempt
                );
            }
        }
    }

    fn tick(&mut self) {
        let current_tick = self.current_tick();
        match self.role {
            Role::Follower => {
                // start election is there's no reply from leader
                if current_tick > self.election_start_point {
                    self.become_candidate();
                }
            }
            Role::Candidate => {
                // restart election if there's not enough vote and there's no leader
                if current_tick > self.election_timeout_point {
                    self.become_candidate();
                }
            }
            Role::Leader => {
                // periodically send heartbeats to followers
                self.heartbeats();
            }
        }

        self.update_rpc_cache();

        if self.apply_message_flag {
            self.apply_message_flag = false;
            self.apply_message();
        }
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role == Role::Leader {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            let term = self.term;
            self.logs_mut().push((term, buf));
            self.debug_logs();
            Ok((self.last_log_index(), term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    // Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
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
    cancel: Arc<AtomicBool>,
    ticker: Arc<Option<JoinHandle<()>>>,
    rpc_reply_ticker: Arc<Option<JoinHandle<()>>>,
}

impl Node {
    // Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        let me = raft.me;
        let (tx, rx) = channel::<(u64, u64, RPCReply)>();
        raft.rpc_reply_channel_tx = Some(tx);
        let raft = Arc::new(Mutex::new(Some(raft)));
        let cancel = Arc::new(AtomicBool::new(false));

        let mut node = Node {
            raft,
            cancel,
            ticker: Arc::new(None),
            rpc_reply_ticker: Arc::new(None),
        };

        // Main ticker thread
        let cancel = node.cancel.clone();
        let raft = node.raft.clone();
        node.ticker = Arc::new(Some(std::thread::spawn(move || {
            info!("#{} started ticking task", me);
            while !cancel.load(SeqCst) {
                {
                    let mut raft = raft.lock().unwrap();
                    if let Some(raft) = raft.as_mut() {
                        raft.tick();
                    } else {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            info!("#{} stopped ticking task", me);
        })));

        // RPC reply handler ticker thread
        let raft = node.raft.clone();
        node.rpc_reply_ticker = Arc::new(Some(std::thread::spawn(move || {
            info!("#{} started checking rpc reply", me);
            for (id, from, reply) in rx.iter() {
                {
                    let mut raft = raft.lock().unwrap();
                    if let Some(raft) = raft.as_mut() {
                        raft.handle_reply(id, from, reply);
                    } else {
                        break;
                    }
                }
                std::thread::yield_now();
            }
            info!("#{} stopped checking rpc reply", me);
        })));
        node
    }

    // the service using Raft (e.g. a k/v server) wants to start
    // agreement on the next command to be appended to Raft's log. if this
    // server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    // the agreement and return immediately. there is no guarantee that this
    // command will ever be committed to the Raft log, since the leader
    // may fail or lose an election. even if the Raft instance has been killed,
    // this function should return gracefully.
    //
    // the first value of the tuple is the index that the command will appear
    // at if it's ever committed. the second is the current term.
    //
    // This method must return without blocking on the raft.
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

    // The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    // Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    // The current state of this peer.
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

    // the tester calls kill() when a Raft instance won't be
    // needed again. you are not required to do anything in
    // kill(), but it might be convenient to (for example)
    // turn off debug output from this instance.
    // In Raft paper, a server crash is a PHYSICAL crash,
    // A.K.A all resources are reset. But we are simulating
    // a VIRTUAL crash in tester, so take care of background
    // threads you generated with this Raft Node.
    pub fn kill(&self) {
        self.cancel.store(true, SeqCst);
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
