# Troubleshooting

The [README](../README.md#when-a-turn-fails) lists the codes a failed turn
carries and what to do about each. This page covers sessions that stay
quarantined.

## A quarantined session

### kagent v1

The broker gives cleanup ten minutes to prove settlement. A prompt still
unproven after that quarantines its session:

- The session accepts no new prompts.
- Orka sees the state `quarantined`, which never counts as settled.
- The broker logs one `kagent session quarantined` line with the owner digest,
  the kagent user and session IDs, and the unfinished task IDs.

Cleanup keeps retrying every 30 seconds, so quarantine ends by itself once
kagent can prove the outcome. The usual cause is an agent that crashed before
kagent recorded the task's end, or a lost kagent controller claim. To resolve it:

1. Use the logged IDs to inspect the task in kagent.
2. If the task cannot finish, delete the kagent session as the logged user.

Deleting the session proves that nothing in it can still change. The broker
then settles the prompt and retires the session normally.

### kagent v0

The broker logs the same `kagent session quarantined` line. Deleting the
session does not resolve a v0 quarantine, because it does not stop a running
turn. What does depends on whether the broker tied the send to its pod:

1. **A send confirmed on its pod.** If the logged tasks are still running,
   cancel them with A2A `CancelTask` as the logged user. If the Agent's pod
   has restarted since, its executions are gone, and the broker's next cancel
   settles the task.
2. **Any other send**, governed or observed, and whether or not it names a
   task: one the broker never confirmed, one whose pod changed during it, and
   one that left no task. Which pod runs it is unknown, so neither a cancel
   nor the task's state proves anything: it settles only once every Agent pod
   that existed at its fence is gone. Restart the Agent's Deployment, and let
   the pods terminate gracefully; never force-delete them.
