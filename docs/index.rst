Welcome to Masenko
------------------

Masenko is an open source (`MIT licensed <https://github.com/husio/masenko/blob/unstable/LICENSE>`_), language agnostic, background task processing manager with a focus on simplicity, reliability and clean interface.


---

* Plain text protocol.
* Tasks are pushed to queue by clients.
* Tasks are explicitly fetched from queues by clients (no subscription).
* Fetched tasks are reserved until acknowledged or leasing client disconnect.
* Tasks that failed or were NACKed are requeued with an exponential backoff.
* Tasks that failed above threshold value are moved to dead letter queue.
* Transaction allows to atomically execute multiple PUSH and ACK instructions.
* All operations are persisted in a WAL file.


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
