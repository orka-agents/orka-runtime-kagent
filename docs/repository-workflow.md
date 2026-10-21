# Repository workflow for wrapped kagent Agents

Status: design. Nothing here is implemented yet.

## Goal

A governed kagent Agent should be able to change a repository the way a
built-in Orka agent does. The work starts from `Task.spec.workspace`, and its
result reaches the forge only through Orka's clean-room publication.

## What must stay true

Orka already guarantees the following, and the wrapper registers these claims
as strict workspace governance:

- **One repository surface.** `Task.spec.workspace` is the only one. The clone
  credential (`readCredentialRef`) and the publication credential
  (`publicationCredentialRef`) never enter the ACP process tree.
- **Orka owns the change.** `orkaOwnedWorkspaceDeltas` and
  `orkaOwnedCleanRoomPublication`: the supervisor captures the workspace delta,
  and the workspace publisher alone creates branches and pull requests.
- **No direct publication.** `noDirectSCMPublication`: nothing the Agent runs
  can push to a forge.

## The problem

The Orka session workspace lives in the wrapper pod, in the ACP child's working
directory. The kagent Agent runs in kagent's sandbox, in another pod, with no
view of that directory. Today a wrapped Agent therefore sees no repository.

## Options

| Option | How it works | Verdict |
| --- | --- | --- |
| A. kagent clones the repository | The Agent uses its own Git credential in its sandbox. | Rejected. It breaks all three claims, and Orka cannot see or publish the change. |
| B. Workspace tools through the relay | The ACP child serves file tools over its own workspace. The broker relays them to kagent like Orka's tools. | Recommended. |
| C. Orka built-in file tools | Orka's `file_read` and `file_write` run in the controller, not in the runtime pod. | Rejected. They do not reach the session workspace. |

## Recommended design (B)

1. **The child serves the workspace.** When governed and the profile's
   workspace intent is `write`, the ACP child starts a loopback MCP server over
   its working directory with a random bearer. It sends that endpoint to the
   broker with each turn, the same way it sends the Orka session MCP server
   today.
2. **The broker relays it.** The relay exposes it at `/workspace` with the same
   turn-token authentication, admission limits, write deadlines, and call
   identity as `/mcp`. The kagent Agent binds a second discovery-disabled
   `RemoteMCPServer` to it. The import step checks that binding like the
   others.
3. **The tool set is small and bounded.**
   - **Tools:** `list_directory`, `read_file`, `search_files`, `write_file`,
     `delete_file`.
   - **No execution:** the harness keeps `skillTools.disableBash`, and there is
     no shell tool.
   - **Path confinement:** every path is resolved under the workspace root
     with no-follow opens, matching Orka's artifact client.
   - **Limits:** reads, writes, and search results have size limits.
4. **Publication is unchanged.** When the prompt settles, the supervisor
   captures the delta and Orka's publisher creates the branch and pull request.
   The Agent never holds a forge credential, so the three claims still hold.
5. **Observed Agents keep read intent.** They see no repository, because their
   tools do not run through the relay.

## Risks and open questions

- **Content outside Orka.** File contents the Agent reads reach kagent's
  conversation history and its model. This is the same data-leakage class the
  governance plan already accepts.
- **Large repositories.** Search runs in the wrapper pod. It needs a time
  limit and a result limit, and very large monorepos may need an index.
- **Binary files.** `read_file` returns text only. Binary files are listed, but
  their content is not returned.
- **Sandbox restarts.** A kagent sandbox that restarts mid-turn may re-issue a
  write on a new MCP session. A rewrite with the same content is harmless; a
  different write is a real second change, and the delta shows it.
- **Workspace class.** Execution workspaces (`Task.spec.execution.workspace`)
  bind a dedicated single-session pool. That pool would have to run the kagent
  wrapper, and it is not qualified for that yet.

## Validation plan

- **Unit tests:** path confinement, covering symlinks, `..`, absolute paths,
  and hard links; size limits; turn-token scoping.
- **Live test:** a governed probe Agent edits one file in a Task with a
  workspace. Orka captures the delta, and the publisher opens a pull request on
  a test repository.
- **Negative test:** an Agent that binds any non-relay tool server is imported
  as observed and gets no workspace tools.
