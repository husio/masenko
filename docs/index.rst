Welcome to Masenko
------------------

Masenko is an open source (`MIT licensed
<https://github.com/husio/masenko/blob/unstable/LICENSE>`_), language agnostic,
background task manager with a focus on reliability and complete, easy to use
API.

Masenko is a standalone server.


Main features of Masenko are:

- Plain text, synchronous protocol.
- Tasks are explicitly fetched from queues by clients (no subscription).
- Fetched tasks are reserved until acknowledged by the client or leasing client
  disconnect.
- Tasks that failed or were ``NACK`` are requeued with an exponential back off.
- Tasks that failed above certain threshold value can be moved to a dead letter
  queue.
- Transaction allows to atomically execute multiple ``PUSH`` and ``ACK``
  instructions.
- All operations are persisted in a Write Ahead Log file.
- Single binary, lightweight server process.

Data persistence
================

Each operation performed by Masenko is atomic and persisted. Masenko ensures
data persistence by maintaining a Write-Ahead-Log file. The current state is
maintained in memory and additionally each change is persisted in the WAL file.
A server restart or crash does not case any data lose.
A maximum WAL file size can be configured. After reaching the limit, a new WAL
file is initialized. Only the most recent WAL file is required to rebuild the
state. Older WAL files can be safely deleted.

Transactions
============

Each operation done within a single request is atomic. Additionally a
transaction can group together multiple operations and ensure multiple
operations are atomically applied. See `ATOMIC` command.


User's Guide
============

.. toctree::
   :maxdepth: 2

   installation
   configuration

   python_client
   go_client

API reference
=============

.. toctree::
   :maxdepth: 2

   commands



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
