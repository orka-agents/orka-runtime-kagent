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
turn:

1. If the logged tasks are still running, cancel them with A2A `CancelTask`
   as the logged user.
2. If the Agent's pod has restarted since, its executions are gone, and the
   broker's next cancel settles the task.
3. An observed send that left no task settles once the Agent's pods that
   existed at the fence are gone; restart the Agent's Deployment.
