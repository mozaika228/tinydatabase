use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};

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
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries_len: u64,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
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
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,
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
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes_granted: HashSet::new(),
        }
    }

    pub fn majority(&self) -> usize {
        let total_nodes = self.peers.len() + 1;
        (total_nodes / 2) + 1
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
            self.become_leader();
        }
    }

    pub fn heartbeat_requests(&self) -> Vec<(u64, AppendEntriesRequest)> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        self.peers
            .iter()
            .map(|p| {
                let next = self.next_index.get(p).copied().unwrap_or(self.last_log_index + 1);
                let prev_log_index = next.saturating_sub(1);
                (
                    *p,
                    AppendEntriesRequest {
                        term: self.current_term,
                        leader_id: self.node_id,
                        prev_log_index,
                        prev_log_term: self.term_for_index(prev_log_index),
                        entries_len: 0,
                        leader_commit: self.commit_index,
                    },
                )
            })
            .collect()
    }

    pub fn append_entry_and_replicate(&mut self) -> Result<Vec<(u64, AppendEntriesRequest)>> {
        if self.role != Role::Leader {
            return Err(Error::InvalidData(
                "only leader can append replicated entries".to_string(),
            ));
        }
        self.last_log_index += 1;
        self.last_log_term = self.current_term;
        self.match_index.insert(self.node_id, self.last_log_index);
        self.next_index.insert(self.node_id, self.last_log_index + 1);

        let mut out = Vec::with_capacity(self.peers.len());
        for peer in &self.peers {
            let next = self
                .next_index
                .get(peer)
                .copied()
                .unwrap_or(self.last_log_index);
            let prev_log_index = next.saturating_sub(1);
            let entries_len = self.last_log_index.saturating_sub(prev_log_index);
            out.push((
                *peer,
                AppendEntriesRequest {
                    term: self.current_term,
                    leader_id: self.node_id,
                    prev_log_index,
                    prev_log_term: self.term_for_index(prev_log_index),
                    entries_len,
                    leader_commit: self.commit_index,
                },
            ));
        }
        Ok(out)
    }

    pub fn on_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index,
            };
        }
        if req.term > self.current_term || self.role != Role::Follower {
            self.step_down(req.term);
        }
        if req.prev_log_index > self.last_log_index {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index,
            };
        }
        self.leader_id = Some(req.leader_id);
        let new_last = req.prev_log_index.saturating_add(req.entries_len);
        if new_last > self.last_log_index {
            self.last_log_index = new_last;
            self.last_log_term = req.term;
        }
        if req.leader_commit > self.commit_index {
            self.commit_index = self.commit_index.max(req.leader_commit.min(self.last_log_index));
        }
        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index,
        }
    }

    pub fn on_append_entries_response(
        &mut self,
        from_peer: u64,
        sent: &AppendEntriesRequest,
        res: AppendEntriesResponse,
    ) {
        if self.role != Role::Leader {
            return;
        }
        if res.term > self.current_term {
            self.step_down(res.term);
            return;
        }
        if res.term < self.current_term {
            return;
        }

        if res.success {
            self.match_index.insert(from_peer, res.match_index);
            self.next_index.insert(from_peer, res.match_index + 1);
            self.advance_commit_index();
        } else {
            let current = self
                .next_index
                .get(&from_peer)
                .copied()
                .unwrap_or(self.last_log_index + 1);
            let backoff = current.saturating_sub(1).max(1);
            self.next_index.insert(from_peer, backoff);
            if sent.entries_len == 0 {
                return;
            }
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
        self.next_index.clear();
        self.match_index.clear();
        self.votes_granted.clear();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.node_id);
        self.next_index.clear();
        self.match_index.clear();
        self.match_index.insert(self.node_id, self.last_log_index);
        self.next_index.insert(self.node_id, self.last_log_index + 1);
        for peer in &self.peers {
            self.next_index.insert(*peer, self.last_log_index + 1);
            self.match_index.insert(*peer, 0);
        }
    }

    fn advance_commit_index(&mut self) {
        let mut matched: Vec<u64> = self.match_index.values().copied().collect();
        if matched.is_empty() {
            return;
        }
        matched.sort_unstable();
        let majority_match = matched[matched.len() - self.majority()];
        if majority_match > self.commit_index && self.term_for_index(majority_match) == self.current_term {
            self.commit_index = majority_match;
        }
    }

    fn term_for_index(&self, idx: u64) -> u64 {
        if idx == 0 {
            0
        } else if idx == self.last_log_index {
            self.last_log_term
        } else {
            self.current_term
        }
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
            prev_log_index: 0,
            prev_log_term: 0,
            entries_len: 0,
            leader_commit: 0,
        });
        assert!(res.success);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 8);
        assert_eq!(node.leader_id, Some(2));
    }

    #[test]
    fn leader_tracks_match_index_and_advances_commit_index() {
        let mut leader = RaftNode::new(1, vec![2, 3]);
        let _ = leader.begin_election().expect("begin election");
        leader.on_vote_response(
            2,
            VoteResponse {
                term: leader.current_term,
                vote_granted: true,
            },
        );
        assert_eq!(leader.role, Role::Leader);

        let reqs = leader.append_entry_and_replicate().expect("append");
        assert_eq!(leader.last_log_index, 1);
        assert_eq!(leader.commit_index, 0);

        let (_peer, req_to_2) = reqs
            .iter()
            .find(|(peer, _)| *peer == 2)
            .cloned()
            .expect("request to peer 2");
        leader.on_append_entries_response(
            2,
            &req_to_2,
            AppendEntriesResponse {
                term: leader.current_term,
                success: true,
                match_index: 1,
            },
        );
        assert_eq!(leader.commit_index, 1);
    }

    #[test]
    fn failed_append_response_backs_off_next_index() {
        let mut leader = RaftNode::new(1, vec![2, 3]);
        let _ = leader.begin_election().expect("begin election");
        leader.on_vote_response(
            2,
            VoteResponse {
                term: leader.current_term,
                vote_granted: true,
            },
        );
        let reqs = leader.append_entry_and_replicate().expect("append");
        let (_, req_to_2) = reqs
            .iter()
            .find(|(peer, _)| *peer == 2)
            .cloned()
            .expect("request to peer 2");
        let before = leader.next_index.get(&2).copied().unwrap_or(0);
        leader.on_append_entries_response(
            2,
            &req_to_2,
            AppendEntriesResponse {
                term: leader.current_term,
                success: false,
                match_index: 0,
            },
        );
        let after = leader.next_index.get(&2).copied().unwrap_or(0);
        assert!(after <= before);
        assert!(after >= 1);
    }
}
