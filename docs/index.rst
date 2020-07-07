Welcome to Masenko
------------------

Masenko is an open source (`MIT licensed <https://github.com/husio/masenko/blob/unstable/LICENSE>`_), language agnostic, background task manager with a focus on reliability and feature completion.


Main features of Masenko are:

- Single binary, lightweight server.
- Plain text, synchronous protocol.
- Tasks are explicitly fetched from queues by clients (no subscription).
- Fetched tasks are reserved until acknowledged or leasing client disconnect.
- Tasks that failed or were NACKed are requeued with an exponential backoff.
- Tasks that failed above certain threshold value can be moved to a dead letter queue.
- Transaction allows to atomically execute multiple PUSH and ACK instructions.
- All operations are persisted in a WAL file.


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
