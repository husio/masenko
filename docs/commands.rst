Commands
--------

`PING`
======

Ping server, receive instant response. This message should be used to ensure the connection health and provide needed heartbeat.

Command: `PING`

Response: `PONG`



`PUSH`
======

Create a new task.

Request: `PUSH {...}`

=============  ==============  ========  ============
 Attribute     Type            Default   Description
=============  ==============  ========  ============
 name          string                    Required. Name of the pushed task
 queue         string          default   Name of the queue that the task is pushed to
 deadqueue     string                    If provided, name of a queue that this task should be moved into after exceeding retry count
 payload       any                       A JSON serilized task payload
 retry         uint            20        How many times a failed task must be retried before removed
 execute_at    string RFC3339            If provided, task cannot be fetched before given date
=============  ==============  ========  ============


Response: `OK {...}`

==========   ========   ==================================
 Attribute   Type       Description
==========   ========   ==================================
 id          uint       The ID of the newly created task.
==========   ========   ==================================



`FETCH`
=======

Fetch a single message from any of requested queues. This calls blocks until a task can be returned or a timeout is reached.

Command: `FETCH { ... }`

===========   ==============   =====================   ======================================================================
 Attribute    Type             Default                 Description
===========   ==============   =====================   ======================================================================
 queues       string array                             Required. A list of queues that a single message should be fetched from. Checked in order provided.
 timeout      string or uint   Half of the heartbeat   A duration after which if no task can be returned, EMPTY response is returned. Must be lower than the client heartbeat.
===========   ==============   =====================   ======================================================================

Response: `EMPTY` - no message is available.

Response: `OK {...}`

==========   =======   ==================================
 Attribute   Type      Description
==========   =======   ==================================
 id          uint      ID of the task.
 queue       string    The name of the queue that the task belongs to.
 name        string    Name of the task.
 payload     any       Task payload.
 failures    uint      Number of failed processing attempts for this task so far.
==========   =======   ==================================

`ACK`
=====

Acknowledge a task with given ID, fetched by the client. Acknowledging marks task as successfully executed and removes it from the queue.

Command: `ACK { ... }`

===========   ========   =====================================================
 Attribute    Type       Description
===========   ========   =====================================================
 id           uint       Required. An ID of a task that should be acknowledged.
===========   ========   =====================================================

Response: `OK {}`

`NACK`
======

Negative acknowledge a task with given ID, fetched by the client. Negative acknowledging marks task as unsuccessfully executed and applies retry logic in order to make it available for future processing.

Command: `NACK { ... }`

===========   ========   =====================================================
 Attribute    Type       Description
===========   ========   =====================================================
 id           uint       Required. An ID of a task that should be negatively acknowledged.
===========   ========   =====================================================

Response: `OK {}`


`INFO`
======

Return the current server state information.

Command: `INFO {}`

Response: `OK {"queues": [ ... ], "metrics": { ... } }`

========================   =============   ============
 Attribute                 Type            Description
========================   =============   ============
 queues                    object array    Set of informations about existing queues.
 queues.name               uint            Name of the queue.
 queues.ready              uint            Number of tasks ready to be fetched.
 queues.delayed            uint            Number of tasks that are scheduled for fetching in the future.
 queues.to_ack             uint            Number of fetched tasks that await acknowledgment from the client.
 metrics                   object          Various global server metrics.
 metrics.clients           uint            Number of currently connected clients.
 metrics.push              uint            Number of PUSH requests handled.
 metrics.fetch             uint            Number of FETCH requests handled.
 metrics.ack               uint            Number of ACK requests handled.
 metrics.nack              uint            Number of NACK requests handled.
 metrics.request           uint            Number of requests handled.
 metrics.request-error     uint            Number of requests that could not be handled due to an error.
========================   =============   ============

`QUIT`
======

Disconnect from the server.

Command: `QUIT {}`

Response: `OK {}`


`ATOMIC` and `DONE`
===================

`ATOMIC` starts a transactions. All commands are read until `DONE` is sent and then executed together. All commands must succeed in order for the change to be persisted.

Only `PUSH` and `ACK` commands are allowed inside of a transaction.

.. code::

   ATOMIC\n
   PUSH {"name": "register-user", "payload": {"name": "John", "admin": false}}
   PUSH {"name": "send-email", "payload": {"to": "john@example.com", "subject": "Hello"}}
   ACK {"id": 123456}
   DONE\n

Response: `OK {}`
