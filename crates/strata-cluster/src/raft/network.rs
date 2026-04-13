//! Raft network implementation — HTTP-based node-to-node communication.
//!
//! Each Raft RPC (AppendEntries, Vote, InstallSnapshot) is sent as a POST
//! request with JSON body to the target node's Raft HTTP endpoint.

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::RaftNetwork;
use reqwest::Client;

use super::types::{NodeId, NodeInfo, TypeConfig};

/// HTTP-based Raft network transport.
///
/// Each instance handles communication with a single remote node.
pub struct NetworkClient {
    addr: String,
    client: Client,
}

impl NetworkClient {
    fn url(&self, path: &str) -> String {
        format!("{}/raft/{}", self.addr.trim_end_matches('/'), path)
    }

    async fn post_json<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp, openraft::error::NetworkError> {
        let resp = self
            .client
            .post(self.url(path))
            .json(req)
            .send()
            .await
            .map_err(|e| openraft::error::NetworkError::new(&e))?;

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| openraft::error::NetworkError::new(&e))?;

        serde_json::from_slice(&bytes).map_err(|e| openraft::error::NetworkError::new(&e))
    }
}

impl RaftNetwork<TypeConfig> for NetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.post_json("append", &rpc)
            .await
            .map_err(RPCError::Network)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, NodeInfo, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.post_json("snapshot", &rpc)
            .await
            .map_err(RPCError::Network)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftError<NodeId>>> {
        self.post_json("vote", &rpc)
            .await
            .map_err(RPCError::Network)
    }
}

/// Factory that creates network clients for each target node.
pub struct NetworkFactory {
    client: Client,
}

impl NetworkFactory {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl openraft::RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, _target: NodeId, node: &NodeInfo) -> NetworkClient {
        NetworkClient {
            addr: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_factory_creation() {
        let _ = NetworkFactory::new();
    }

    #[test]
    fn network_client_url() {
        let client = NetworkClient {
            addr: "http://10.0.0.1:9433".into(),
            client: Client::new(),
        };
        assert_eq!(client.url("append"), "http://10.0.0.1:9433/raft/append");
        assert_eq!(client.url("vote"), "http://10.0.0.1:9433/raft/vote");
    }
}
