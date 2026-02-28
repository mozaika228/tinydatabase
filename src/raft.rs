use crate::error::{Error, Result};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct RaftNode {
    pub node_id: u64,
    pub peers: Vec<u64>,
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    votes_granted: HashSet<u64>,
}

impl RaftNode {
    pub fn new(node_id: u64, peers: Vec<u64>) -> Self {
        Self {
            node_id,
            peers,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            last_log_index: 0,
            last_log_term: 0,
            votes_granted: HashSet::new(),
        }
    }

    pub fn majority(&self) -> usize {
        (self.peers.len() + 1).div_ceil(2)
    }

    pub fn begin_election(&mut self) -> Result<Vec<(u64, VoteRequest)>> {
        if self.peers.is_empty() {
            return Err(Error::InvalidData("raft node has no peer config".to_string()));
        }
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id);
        self.leader_id = None;
        self.votes_granted.clear();
        self.votes_granted.insert(self.node_id);
        let req = VoteRequest {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_index: self.last_log_index,
            last_log_term: self.last_log_term,
        };
        Ok(self.peers.iter().map(|p| (*p, req.clone())).collect())
    }

    pub fn on_vote_request(&mut self, req: VoteRequest) -> VoteResponse {
        if req.term < self.current_term {
            return VoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if req.term > self.current_term {
            self.step_down(req.term);
        }

        let up_to_date = (req.last_log_term > self.last_log_term)
            || (req.last_log_term == self.last_log_term && req.last_log_index >= self.last_log_index);
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);
        let granted = can_vote && up_to_date;
        if granted {
            self.voted_for = Some(req.candidate_id);
        }
        VoteResponse {
            term: self.current_term,
            vote_granted: granted,
        }
    }

    pub fn on_vote_response(&mut self, from_peer: u64, res: VoteResponse) {
        if self.role != Role::Candidate {
            return;
        }
        if res.term > self.current_term {
            self.step_down(res.term);
            return;
        }
        if res.term < self.current_term || !res.vote_granted {
            return;
        }
        self.votes_granted.insert(from_peer);
        if self.votes_granted.len() >= self.majority() {
            self.role = Role::Leader;
            self.leader_id = Some(self.node_id);
        }
    }

    pub fn heartbeat_requests(&self) -> Vec<(u64, AppendEntriesRequest)> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        let req = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.node_id,
        };
        self.peers.iter().map(|p| (*p, req.clone())).collect()
    }

    pub fn on_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }
        if req.term > self.current_term || self.role != Role::Follower {
            self.step_down(req.term);
        }
        self.leader_id = Some(req.leader_id);
        AppendEntriesResponse {
            term: self.current_term,
            success: true,
        }
    }

    pub fn set_log_tip(&mut self, index: u64, term: u64) {
        self.last_log_index = index;
        self.last_log_term = term;
    }

    fn step_down(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.votes_granted.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn election_becomes_leader_on_majority() {
        let mut n = RaftNode::new(1, vec![2, 3, 4]);
        let _ = n.begin_election().expect("begin election");
        assert_eq!(n.role, Role::Candidate);
        n.on_vote_response(
            2,
            VoteResponse {
                term: n.current_term,
                vote_granted: true,
            },
        );
        assert_eq!(n.role, Role::Candidate);
        n.on_vote_response(
            3,
            VoteResponse {
                term: n.current_term,
                vote_granted: true,
            },
        );
        assert_eq!(n.role, Role::Leader);
    }

    #[test]
    fn vote_request_respects_log_freshness() {
        let mut follower = RaftNode::new(2, vec![1, 3]);
        follower.set_log_tip(10, 3);
        let res_stale = follower.on_vote_request(VoteRequest {
            term: 5,
            candidate_id: 1,
            last_log_index: 9,
            last_log_term: 3,
        });
        assert!(!res_stale.vote_granted);
        let res_fresh = follower.on_vote_request(VoteRequest {
            term: 5,
            candidate_id: 1,
            last_log_index: 10,
            last_log_term: 3,
        });
        assert!(res_fresh.vote_granted);
    }

    #[test]
    fn append_entries_higher_term_forces_step_down() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.role = Role::Leader;
        node.current_term = 7;
        let res = node.on_append_entries(AppendEntriesRequest {
            term: 8,
            leader_id: 2,
        });
        assert!(res.success);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 8);
        assert_eq!(node.leader_id, Some(2));
    }
}

