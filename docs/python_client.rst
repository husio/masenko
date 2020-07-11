Python client
-------------


.. todo::

   This is a draft document.

.. module:: client

.. autofunction:: connect

.. note::

   In most cases, you want to use :class:`Client` to connect to a Masenko
   server. :class:`BareClient` does is a thin wrapper over a socket and does
   not include healthcheck worker.


.. autoclass:: Client
   :members:
   :inherited-members:


.. autoclass:: Transaction
   :members:


.. autoclass:: BareClient
   :members:
   :inherited-members:



.. autoexception:: Error
.. autoexception:: EmptyError
.. autoexception:: UnexpectedResponseError
.. autoexception:: ResponseError
