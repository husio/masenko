Python client
-------------


.. todo::

   This is a draft document.









.. module:: client


.. autofunction:: connect

Use `connect` when you want to limit the livetime of a client connection to a
certain scope::

     with connect("localhost", 12345) as masenko:
         masenko.push("register-fruit", {"age": 2, "color": "blue"})


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

